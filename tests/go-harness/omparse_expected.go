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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

// omParseInputGlobs match the OpenMetrics fixtures, relative to testdata/. The
// textparse/ files are synced from upstream; the omerrors/ files are
// hand-written error cases.
var omParseInputGlobs = []string{
	"textparse/*.om.txt",
	"omerrors/*.om.txt",
}

// writeOMParseExpected writes each input's entries twice: to testdata/omparse/
// with the scrape default (_created lines are series), and to
// testdata/omparse_st/ with _created skipped and a start timestamp per series.
func writeOMParseExpected(outRoot string) ([]string, error) {
	var inputs []string
	for _, glob := range omParseInputGlobs {
		matches, err := filepath.Glob(filepath.Join(outRoot, glob))
		if err != nil {
			return nil, fmt.Errorf("glob %s: %w", glob, err)
		}
		for _, match := range matches {
			rel, err := filepath.Rel(outRoot, match)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, filepath.ToSlash(rel))
		}
	}
	sort.Strings(inputs)

	var written []string
	for _, input := range inputs {
		data, err := os.ReadFile(filepath.Join(outRoot, input))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", input, err)
		}
		for _, outDir := range []string{"omparse", "omparse_st"} {
			dstPath := filepath.Join(outRoot, outDir, input)
			if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
				return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(dstPath), err)
			}
			rendered := renderOMParse(data, outDir == "omparse_st")
			if err := os.WriteFile(dstPath, []byte(rendered), 0o644); err != nil {
				return nil, fmt.Errorf("write %s: %w", dstPath, err)
			}
			written = append(written, outDir+"/"+input)
		}
	}
	return written, nil
}

func renderOMParse(data []byte, startTimestamps bool) string {
	var out strings.Builder
	var opts []textparse.OpenMetricsOption
	if startTimestamps {
		opts = append(opts, textparse.WithOMParserSTSeriesSkipped())
	}
	parser := textparse.NewOpenMetricsParser(data, labels.NewSymbolTable(), opts...)
	var labelSet labels.Labels
	for {
		entry, err := parser.Next()
		if errors.Is(err, io.EOF) {
			return out.String()
		}
		if err != nil {
			out.WriteString("error\n")
			return out.String()
		}
		switch entry {
		case textparse.EntryHelp:
			name, text := parser.Help()
			fmt.Fprintf(&out, "help %s %s\n", quote(name), quote(text))
		case textparse.EntryType:
			name, metricType := parser.Type()
			fmt.Fprintf(&out, "type %s %s\n", quote(name), metricType)
		case textparse.EntryUnit:
			name, unit := parser.Unit()
			fmt.Fprintf(&out, "unit %s %s\n", quote(name), quote(unit))
		case textparse.EntryComment:
			fmt.Fprintf(&out, "comment %s\n", quote(parser.Comment()))
		case textparse.EntrySeries:
			series, timestampMillis, value := parser.Series()
			timestamp := "-"
			if timestampMillis != nil {
				timestamp = strconv.FormatInt(*timestampMillis, 10)
			}
			fmt.Fprintf(&out, "series %s %016x %s", quote(series), math.Float64bits(value), timestamp)
			parser.Labels(&labelSet)
			labelSet.Range(func(label labels.Label) {
				fmt.Fprintf(&out, " %s=%s", quote([]byte(label.Name)), quote([]byte(label.Value)))
			})
			out.WriteString("\n")
			var ex exemplar.Exemplar
			if parser.Exemplar(&ex) {
				exemplarTimestamp := "-"
				if ex.HasTs {
					exemplarTimestamp = strconv.FormatInt(ex.Ts, 10)
				}
				fmt.Fprintf(&out, "exemplar %016x %s", math.Float64bits(ex.Value), exemplarTimestamp)
				ex.Labels.Range(func(label labels.Label) {
					fmt.Fprintf(&out, " %s=%s", quote([]byte(label.Name)), quote([]byte(label.Value)))
				})
				out.WriteString("\n")
			}
			// StartTimestamp peeks ahead and overwrites the series, offsets and
			// value, so it runs last. Without WithOMParserSTSeriesSkipped it
			// also turns off _created skipping, so it only runs in st mode.
			if startTimestamps {
				fmt.Fprintf(&out, "st %d\n", parser.StartTimestamp())
			}
		}
	}
}
