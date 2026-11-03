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
	"fmt"
	"path/filepath"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

// omParseInputGlobs match the OpenMetrics fixtures, relative to testdata/. The
// textparse/ files are synced from upstream; the omerrors/ files are
// hand-written error cases copied from tests/go-harness/testdata/omerrors/.
var omParseInputGlobs = []string{
	"textparse/*.om.txt",
	"omerrors/*.om.txt",
}

// writeOMParseExpected writes each input's entries to testdata/omparse/, with
// _created lines skipped and a start timestamp per series.
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
		err := writeExpected(outRoot, "omparse", input, func(data []byte) string {
			parser := textparse.NewOpenMetricsParser(data, labels.NewSymbolTable(), textparse.WithOMParserSTSeriesSkipped())
			return renderEntries(parser, true)
		})
		if err != nil {
			return nil, err
		}
		written = append(written, "omparse/"+input)
	}
	return written, nil
}
