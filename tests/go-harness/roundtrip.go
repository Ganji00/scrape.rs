// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// roundTripFamilies are the canonical MetricFamily inputs that the round-trip fixtures encode.
// They cover every io.prometheus.client type that Prometheus text 0.0.4 and OpenMetrics 1.0 can
// carry, UTF-8 metric and label names, escaped label values and help text, exemplars, created
// timestamps, units, sample timestamps, and special float values. Labels are sorted by name,
// summary quantiles and histogram buckets ascend, and every histogram ends with a +Inf bucket,
// so that the inputs are already in the form that decoders return.
var roundTripFamilies = []string{
	`name: "http_requests_total"
help: "Total HTTP requests.\nSecond line with a \\ backslash."
type: COUNTER
metric: {
  label: { name: "code" value: "200" }
  label: { name: "method" value: "GET" }
  counter: {
    value: 1027
    exemplar: {
      label: { name: "span_id" value: "0123456789abcdef" }
      label: { name: "trace_id" value: "0123456789abcdef0123456789abcdef" }
      value: 1
      timestamp: { seconds: 1700000000 nanos: 500000000 }
    }
    created_timestamp: { seconds: 500 nanos: 250000000 }
  }
  timestamp_ms: 1700000000123
}
metric: {
  label: { name: "code" value: "400" }
  label: { name: "method" value: "POST" }
  counter: { value: 3 }
}`,
	`name: "my.events_total"
help: "Counter with a UTF-8 name."
type: COUNTER
metric: {
  label: { name: "event.kind" value: "click" }
  counter: { value: 42 created_timestamp: { seconds: 1000 } }
}`,
	`name: "process_temperature_celsius"
help: "Gauge with special values."
type: GAUGE
unit: "celsius"
metric: {
  label: { name: "sensor" value: "a" }
  gauge: { value: -1.5 }
}
metric: {
  label: { name: "sensor" value: "b" }
  gauge: { value: 1e-300 }
}
metric: {
  label: { name: "sensor" value: "c" }
  gauge: { value: inf }
}
metric: {
  label: { name: "sensor" value: "d" }
  gauge: { value: -inf }
}
metric: {
  label: { name: "sensor" value: "e" }
  gauge: { value: 123456789012345 }
  timestamp_ms: 1000
}`,
	`name: "label_escapes"
help: "Label values with \"quotes\", \\ backslashes, and\nnewlines."
type: GAUGE
metric: {
  label: { name: "path" value: "C:\\Temp\\\"x\"\nline ✓" }
  gauge: { value: 1 }
}`,
	`name: "http.server.request.duration_seconds"
help: "Histogram with a UTF-8 name."
type: HISTOGRAM
unit: "seconds"
metric: {
  label: { name: "http.method" value: "GET" }
  label: { name: "route" value: "/" }
  histogram: {
    sample_count: 4
    sample_sum: 2.5
    bucket: {
      cumulative_count: 1
      upper_bound: 0.1
      exemplar: {
        label: { name: "pod" value: "a" }
        value: 0.05
        timestamp: { seconds: 123 nanos: 456000000 }
      }
    }
    bucket: {
      cumulative_count: 3
      upper_bound: 1
      exemplar: {
        label: { name: "pod" value: "b" }
        value: 0.5
      }
    }
    bucket: { cumulative_count: 4 upper_bound: inf }
    created_timestamp: { seconds: 1000 nanos: 500000000 }
  }
}`,
	`name: "rpc_duration_seconds"
help: "Summary."
type: SUMMARY
metric: {
  label: { name: "service" value: "a" }
  summary: {
    sample_count: 5
    sample_sum: 10
    quantile: { quantile: 0.5 value: 1 }
    quantile: { quantile: 0.99 value: 2 }
    created_timestamp: { seconds: 2000 }
  }
}
metric: {
  label: { name: "service" value: "b" }
  summary: { sample_count: 0 sample_sum: 0 }
  timestamp_ms: 1700000000000
}`,
	`name: "queue_size"
help: "Gauge histogram."
type: GAUGE_HISTOGRAM
metric: {
  histogram: {
    sample_count: 3
    sample_sum: 1
    bucket: { cumulative_count: 2 upper_bound: 1 }
    bucket: { cumulative_count: 3 upper_bound: inf }
  }
}`,
	`name: "mystery"
type: UNTYPED
metric: { untyped: { value: 1 } }
metric: {
  label: { name: "x" value: "y" }
  untyped: { value: -2 }
  timestamp_ms: 5000
}`,
}

// roundTripFormat is an expfmt format that the round-trip fixtures cover.
type roundTripFormat struct {
	// name prefixes the fixture file names.
	name   string
	format expfmt.Format
}

var roundTripFormats = []roundTripFormat{
	{name: "text-0.0.4", format: expfmt.FmtText},
	{name: "openmetrics-1.0.0", format: expfmt.FmtOpenMetrics_1_0_0},
}

var roundTripEscapingSchemes = []model.EscapingScheme{
	model.NoEscaping,
	model.UnderscoreEscaping,
	model.DotsEscaping,
	model.ValueEncodingEscaping,
}

const roundTripDir = "roundtrip"

// roundTripCase is one format and escaping scheme combination of the round-trip fixtures.
type roundTripCase struct {
	format roundTripFormat
	scheme model.EscapingScheme
}

func (c roundTripCase) contentType() expfmt.Format {
	return expfmt.Format(string(c.format.format) + "; " + model.EscapingKey + "=" + c.scheme.String())
}

// fileName is the fixture path relative to testdata/, without the extension.
func (c roundTripCase) fileName() string {
	return roundTripDir + "/" + c.format.name + "." + c.scheme.String()
}

func roundTripCases() []roundTripCase {
	var cases []roundTripCase
	for _, format := range roundTripFormats {
		for _, scheme := range roundTripEscapingSchemes {
			cases = append(cases, roundTripCase{format: format, scheme: scheme})
		}
	}
	return cases
}

func parseRoundTripFamilies() ([]*dto.MetricFamily, error) {
	families := make([]*dto.MetricFamily, 0, len(roundTripFamilies))
	for _, text := range roundTripFamilies {
		family := &dto.MetricFamily{}
		if err := prototext.Unmarshal([]byte(text), family); err != nil {
			return nil, fmt.Errorf("unmarshal round-trip family: %w", err)
		}
		families = append(families, family)
	}
	return families, nil
}

// writeRoundTrip encodes roundTripFamilies with expfmt.NewEncoder for every round-trip case.
// It writes the encoded payload to testdata/roundtrip/<format>.<scheme>.txt and the families
// that a decoder must return for it, as delimited protobuf, to
// testdata/roundtrip/<format>.<scheme>.pb.
func writeRoundTrip(outRoot string) ([]string, error) {
	families, err := parseRoundTripFamilies()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(outRoot, roundTripDir), 0o755); err != nil {
		return nil, err
	}
	var written []string
	for _, c := range roundTripCases() {
		payload, err := encodeRoundTrip(c, families)
		if err != nil {
			return nil, err
		}
		var expected bytes.Buffer
		for _, family := range families {
			if _, err := protodelim.MarshalTo(&expected, expectedRoundTripFamily(c, family)); err != nil {
				return nil, fmt.Errorf("%s: marshal %s: %w", c.fileName(), family.GetName(), err)
			}
		}
		for ext, data := range map[string][]byte{".txt": payload, ".pb": expected.Bytes()} {
			name := c.fileName() + ext
			if err := os.WriteFile(filepath.Join(outRoot, name), data, 0o644); err != nil {
				return nil, err
			}
			written = append(written, name)
		}
	}
	return written, nil
}

func encodeRoundTrip(c roundTripCase, families []*dto.MetricFamily) ([]byte, error) {
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, c.contentType(), expfmt.WithCreatedLines())
	for _, family := range families {
		if err := enc.Encode(family); err != nil {
			return nil, fmt.Errorf("%s: encode %s: %w", c.fileName(), family.GetName(), err)
		}
	}
	if closer, ok := enc.(expfmt.Closer); ok {
		if err := closer.Close(); err != nil {
			return nil, fmt.Errorf("%s: close: %w", c.fileName(), err)
		}
	}
	return buf.Bytes(), nil
}

// expectedRoundTripFamily returns the family that decoding the encoded form of family yields. The
// encoder escapes metric and label names that are not valid legacy names with the case's escaping
// scheme. Prometheus text 0.0.4 has no units, exemplars, created timestamps, or gauge histograms,
// so they are dropped and gauge histograms become histograms.
func expectedRoundTripFamily(c roundTripCase, family *dto.MetricFamily) *dto.MetricFamily {
	out := proto.Clone(model.EscapeMetricFamily(family, c.scheme)).(*dto.MetricFamily)
	for _, metric := range out.Metric {
		sort.Slice(metric.Label, func(i, j int) bool {
			return metric.Label[i].GetName() < metric.Label[j].GetName()
		})
	}
	if c.format.format != expfmt.FmtText {
		return out
	}
	out.Unit = nil
	if out.GetType() == dto.MetricType_GAUGE_HISTOGRAM {
		out.Type = dto.MetricType_HISTOGRAM.Enum()
	}
	for _, metric := range out.Metric {
		if counter := metric.Counter; counter != nil {
			counter.Exemplar = nil
			counter.CreatedTimestamp = nil
		}
		if summary := metric.Summary; summary != nil {
			summary.CreatedTimestamp = nil
		}
		if histogram := metric.Histogram; histogram != nil {
			histogram.CreatedTimestamp = nil
			for _, bucket := range histogram.Bucket {
				bucket.Exemplar = nil
			}
		}
	}
	return out
}
