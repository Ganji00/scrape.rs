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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	dto "github.com/prometheus/prometheus/prompb/io/prometheus/client"
)

const protobufParseInput = "textparse/TestProtobufParse.pb"

// protobufParseInputs are the delimited protobuf fixtures, relative to testdata/.
var protobufParseInputs = []string{
	protobufParseInput,
	"expfmt/protobuf",
	"expfmt/protobuf-multimessage",
}

func writeProtobufParseInput(outRoot, srcFile string) (string, error) {
	families, err := extractFuncStringSlice(srcFile, "createTestProtoBuf", "testMetricFamilies")
	if err != nil {
		return "", err
	}
	var out []byte
	for _, text := range families {
		var family dto.MetricFamily
		if err := proto.UnmarshalText(text, &family); err != nil {
			return "", fmt.Errorf("unmarshal text: %w", err)
		}
		data, err := proto.Marshal(&family)
		if err != nil {
			return "", fmt.Errorf("marshal %s: %w", family.GetName(), err)
		}
		out = binary.AppendUvarint(out, uint64(len(data)))
		out = append(out, data...)
	}
	if err := os.WriteFile(filepath.Join(outRoot, protobufParseInput), out, 0o644); err != nil {
		return "", err
	}
	return protobufParseInput, nil
}

// writeProtobufParseExpected runs textparse.NewProtobufParser on each input with
// its default options and writes one line per entry to testdata/protobufparse/.
func writeProtobufParseExpected(outRoot string) ([]string, error) {
	var written []string
	for _, input := range protobufParseInputs {
		err := writeExpected(outRoot, "protobufparse", input, func(data []byte) string {
			parser := textparse.NewProtobufParser(data, false, false, false, false, labels.NewSymbolTable())
			return renderEntries(&nativeHistogramsOnly{Parser: parser}, true)
		})
		if err != nil {
			return nil, err
		}
		written = append(written, "protobufparse/"+input)
	}
	return written, nil
}

// nativeHistogramsOnly drops the classic series of histograms, because the Rust parser only
// returns native histograms.
type nativeHistogramsOnly struct {
	textparse.Parser
	histogramFamily bool
}

func (p *nativeHistogramsOnly) Next() (textparse.Entry, error) {
	for {
		entry, err := p.Parser.Next()
		if err != nil {
			return entry, err
		}
		switch entry {
		case textparse.EntryType:
			_, metricType := p.Type()
			p.histogramFamily = metricType == model.MetricTypeHistogram || metricType == model.MetricTypeGaugeHistogram
		case textparse.EntrySeries:
			if p.histogramFamily {
				continue
			}
		}
		return entry, nil
	}
}
