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
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/protobuf/proto"
)

// TestRoundTripTextMatchesTextParser checks that the expected families of every Prometheus text
// round-trip case are what expfmt.TextParser returns for the encoded payload.
func TestRoundTripTextMatchesTextParser(t *testing.T) {
	families, err := parseRoundTripFamilies()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range roundTripCases() {
		if c.format.format != expfmt.FmtText {
			continue
		}
		t.Run(c.fileName(), func(t *testing.T) {
			payload, err := encodeRoundTrip(c, families)
			if err != nil {
				t.Fatal(err)
			}
			parser := expfmt.NewTextParser(model.UTF8Validation)
			parsed, err := parser.TextToMetricFamilies(bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			if len(parsed) != len(families) {
				t.Fatalf("got %d families, want %d", len(parsed), len(families))
			}
			for _, family := range families {
				want := expectedRoundTripFamily(c, family)
				got, ok := parsed[want.GetName()]
				if !ok {
					t.Fatalf("missing family %q", want.GetName())
				}
				if !proto.Equal(got, want) {
					t.Errorf("family %q:\ngot  %v\nwant %v", want.GetName(), got, want)
				}
			}
		})
	}
}
