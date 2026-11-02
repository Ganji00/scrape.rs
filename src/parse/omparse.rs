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

//! OpenMetrics 1.0 text format parser, a port of
//! [`openmetricsparse.go`](https://github.com/prometheus/prometheus/blob/main/model/textparse/openmetricsparse.go).

use std::borrow::Cow;

use super::float::{FloatBuf, common_openmetrics_float, format_openmetrics_float, parse_go_float};
use super::omlex::{Lexer, Token};
use super::{
    BUCKET_LABEL, Entry, LabelsBuffer, METRIC_NAME_LABEL, MetricType, ParseError, Parser,
    QUANTILE_LABEL,
};

/// Go's `value.NormalNaN`, the canonical NaN bit pattern Prometheus stores.
const NORMAL_NAN: u64 = 0x7ff8_0000_0000_0001;

const CREATED_SUFFIX: &[u8] = b"_created";

/// Zero-copy parser for the OpenMetrics 1.0 text exposition format.
///
/// Mirrors Go's `textparse.NewOpenMetricsParser` with Prometheus's default scrape options:
/// `_created` lines are returned as series, and type and unit labels are disabled.
pub struct OpenMetricsParser<'a> {
    lexer: Lexer<'a>,
    skip_created_series: bool,
    /// Set while peeking ahead for a `_created` line, where Go skips exemplars unparsed.
    ignore_exemplar: bool,
    line_start: usize,
    series: &'a [u8],
    text: &'a [u8],
    /// Start and end of the metric name in the input.
    name: Option<(usize, usize)>,
    /// Length of the name on the last metadata line, Go's `mfNameLen`.
    family_name_len: usize,
    /// The type from the last `# TYPE` line, which Go keeps across metric families.
    metric_type: MetricType,
    value: f64,
    timestamp: Option<i64>,
    /// Escaped label names and values, without quotes. Values are already checked to be UTF-8.
    labels: Vec<(&'a [u8], &'a [u8])>,
    exemplar: Option<(f64, Option<i64>)>,
    exemplar_labels: Vec<(&'a [u8], &'a [u8])>,
    /// Labels buffer lent to the start timestamp lookahead, so it does not allocate per call.
    peek_labels: Vec<(&'a [u8], &'a [u8])>,
    /// Go's start timestamp cache: the series key and start timestamp of the last hit.
    start_timestamp_key: Vec<u8>,
    start_timestamp: Option<i64>,
}

impl<'a> OpenMetricsParser<'a> {
    /// Creates a parser over `input`.
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            skip_created_series: false,
            ignore_exemplar: false,
            line_start: 0,
            series: b"",
            text: b"",
            name: None,
            family_name_len: 0,
            metric_type: MetricType::Unknown,
            value: 0.0,
            timestamp: None,
            labels: Vec::new(),
            exemplar: None,
            exemplar_labels: Vec::new(),
            peek_labels: Vec::new(),
            start_timestamp_key: Vec::new(),
            start_timestamp: None,
        }
    }

    /// Skips `_created` lines of counters, summaries, and histograms, so they only feed
    /// [`Parser::start_timestamp`]. Go's `WithOMParserSTSeriesSkipped`.
    pub fn skip_created_series(mut self) -> Self {
        self.skip_created_series = true;
        self
    }

    fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            expected,
            offset: self.lexer.token_start,
        }
    }

    fn parse_metadata(&mut self, kind: Token) -> Result<Entry, ParseError> {
        if self.lexer.lex() != Token::MName {
            return Err(self.unexpected("expected metric name"));
        }
        let (mut start, mut end) = (self.lexer.token_start, self.lexer.pos);
        if self.lexer.input[start] == b'"' && self.lexer.input[end - 1] == b'"' {
            start += 1;
            end -= 1;
        }
        self.family_name_len = end - start;
        self.name = Some((start, end));
        if self.lexer.lex() != Token::Text {
            return Err(self.unexpected("expected text"));
        }
        let text = self.lexer.token();
        // The text token starts with a blank and ends with a linebreak.
        self.text = text.get(1..text.len() - 1).unwrap_or_default();
        match kind {
            Token::Type => {
                self.metric_type = parse_metric_type(self.text)?;
                Ok(Entry::Type)
            }
            Token::Help => {
                if std::str::from_utf8(self.text).is_err() {
                    return Err(ParseError::InvalidUtf8 {
                        offset: self.lexer.token_start,
                    });
                }
                Ok(Entry::Help)
            }
            _ => {
                let metric = self.metric_name();
                let unit = self.text;
                let is_suffix = metric.len() > unit.len()
                    && metric.ends_with(unit)
                    && metric[metric.len() - unit.len() - 1] == b'_';
                if !unit.is_empty() && !is_suffix {
                    return Err(ParseError::InvalidUnit {
                        metric: String::from_utf8_lossy(metric).into_owned(),
                        unit: String::from_utf8_lossy(unit).into_owned(),
                    });
                }
                Ok(Entry::Unit)
            }
        }
    }

    /// Parses a sample line that starts with a metric name or with `{`.
    fn parse_series(&mut self, first: Token) -> Result<Entry, ParseError> {
        let mut token = first;
        if first == Token::MName {
            self.name = Some((self.line_start, self.lexer.pos));
            token = self.lexer.lex();
        }
        if token == Token::BraceOpen {
            self.parse_label_values(false)?;
            token = self.lexer.lex();
        }
        self.series = &self.lexer.input[self.line_start..self.lexer.token_start];
        self.parse_series_end_of_line(token)?;
        Ok(Entry::Series)
    }

    /// Parses labels up to and including `}`, into the exemplar labels when `exemplar` is set.
    fn parse_label_values(&mut self, exemplar: bool) -> Result<(), ParseError> {
        let mut token = self.lexer.lex();
        loop {
            let (mut start, mut end) = (self.lexer.token_start, self.lexer.pos);
            let quoted = match token {
                Token::BraceClose => return Ok(()),
                Token::LName => false,
                Token::QString => true,
                _ => return Err(self.unexpected("expected label name")),
            };
            token = self.lexer.lex();
            if quoted && matches!(token, Token::Comma | Token::BraceClose) {
                if exemplar {
                    return Err(self.unexpected("expected label name"));
                }
                if self.name.is_some() {
                    return Err(ParseError::InvalidMetricName {
                        offset: self.line_start,
                    });
                }
                self.name = Some((start + 1, end - 1));
                if token == Token::BraceClose {
                    return Ok(());
                }
                token = self.lexer.lex();
                continue;
            }
            if quoted {
                start += 1;
                end -= 1;
            }
            if token != Token::Equal {
                return Err(self.unexpected("expected equal"));
            }
            if self.lexer.lex() != Token::LValue {
                return Err(self.unexpected("expected label value"));
            }
            let value = self.lexer.token();
            if std::str::from_utf8(value).is_err() {
                return Err(ParseError::InvalidUtf8 {
                    offset: self.lexer.token_start,
                });
            }
            // The token can end in NUL bytes after the closing quote. Go drops only the last
            // byte, so it keeps the quote and all but the last NUL in the value.
            let close = value.iter().rposition(|&byte| byte == b'"').unwrap_or(0);
            let label = (&self.lexer.input[start..end], &value[1..close.max(1)]);
            if exemplar {
                self.exemplar_labels.push(label);
            } else {
                self.labels.push(label);
            }
            token = self.lexer.lex();
            match token {
                Token::Comma => token = self.lexer.lex(),
                Token::BraceClose => {}
                _ => return Err(self.unexpected("expected comma or brace close")),
            }
        }
    }

    fn parse_series_end_of_line(&mut self, token: Token) -> Result<(), ParseError> {
        if self.name.is_none() {
            return Err(ParseError::InvalidMetricName {
                offset: self.line_start,
            });
        }
        self.value = self.float_value(token, "expected value after metric")?;
        self.timestamp = None;
        match self.lexer.lex() {
            Token::Eof => return Err(ParseError::MissingEof),
            Token::Comment => self.parse_exemplar()?,
            Token::Timestamp => {
                self.timestamp = Some(self.timestamp_value()?);
                match self.lexer.lex() {
                    Token::Linebreak => {}
                    Token::Comment => self.parse_exemplar()?,
                    _ => return Err(self.unexpected("expected next entry after timestamp")),
                }
            }
            // Go has no default case here, so any other token ends the sample without error.
            _ => {}
        }
        Ok(())
    }

    /// Parses the exemplar after ` # {`, Go's `parseComment`.
    fn parse_exemplar(&mut self) -> Result<(), ParseError> {
        if self.ignore_exemplar {
            loop {
                let before = self.lexer.pos;
                match self.lexer.lex() {
                    Token::Linebreak => return Ok(()),
                    Token::Eof => return Err(ParseError::MissingEof),
                    // Go loops forever here, so a scrape target could stall the scraper.
                    Token::Invalid if self.lexer.pos == before => {
                        return Err(self.unexpected("expected linebreak after exemplar"));
                    }
                    _ => {}
                }
            }
        }
        self.parse_label_values(true)?;
        let token = self.lexer.lex();
        let value = self.float_value(token, "expected value after exemplar labels")?;
        let mut timestamp = None;
        match self.lexer.lex() {
            Token::Eof => return Err(ParseError::MissingEof),
            Token::Linebreak => {}
            Token::Timestamp => {
                timestamp = Some(self.timestamp_value()?);
                if self.lexer.lex() != Token::Linebreak {
                    return Err(self.unexpected("expected next entry after exemplar timestamp"));
                }
            }
            _ => return Err(self.unexpected("expected timestamp or comment")),
        }
        self.exemplar = Some((value, timestamp));
        Ok(())
    }

    /// Parses the current value token, which starts with a blank.
    fn float_value(&self, token: Token, expected: &'static str) -> Result<f64, ParseError> {
        if token != Token::Value {
            return Err(self.unexpected(expected));
        }
        let raw = &self.lexer.token()[1..];
        let value = parse_sample_value(raw)
            .ok_or_else(|| ParseError::InvalidValue(String::from_utf8_lossy(raw).into_owned()))?;
        Ok(if value.is_nan() {
            f64::from_bits(NORMAL_NAN)
        } else {
            value
        })
    }

    /// Parses the current timestamp token, float seconds after a blank, into milliseconds.
    fn timestamp_value(&self) -> Result<i64, ParseError> {
        let raw = &self.lexer.token()[1..];
        let seconds = parse_sample_value(raw)
            .filter(|seconds| seconds.is_finite())
            .ok_or_else(|| {
                ParseError::InvalidTimestamp(String::from_utf8_lossy(raw).into_owned())
            })?;
        // NOTE: Rust saturates out-of-range casts, Go's int64() is platform-specific there.
        Ok((seconds * 1000.0) as i64)
    }

    fn metric_name(&self) -> &'a [u8] {
        self.name
            .map_or(&[][..], |(start, end)| &self.lexer.input[start..end])
    }

    fn is_created_series(&self) -> bool {
        requires_start_timestamp(self.metric_type) && self.metric_name().ends_with(CREATED_SUFFIX)
    }

    /// Go's `seriesHash` input: label names and values, without `le` on histograms and
    /// `quantile` on summaries, followed by the metric family name.
    fn series_key<'k>(&'k self, family: &'k [u8]) -> impl Iterator<Item = u8> + 'k {
        self.labels
            .iter()
            .filter(|(name, _)| !is_float_label(self.metric_type, name))
            .flat_map(|(name, value)| name.iter().chain(value.iter()))
            .chain(family)
            .copied()
    }

    /// The metric family name Go derives from the last metadata line, `currName` in Go.
    fn family_name(&self) -> &'a [u8] {
        let input = self.lexer.input;
        let (name_start, _) = self.name.unwrap_or_default();
        let quoted = self.series.starts_with(b"{\"");
        let end = self.line_start + self.family_name_len + if quoted { 2 } else { 0 };
        // Go slices past the series into the rest of the input when the family name is
        // longer than the series, and panics when it would end before the name starts.
        let end = end.min(input.len());
        &input[name_start.min(end)..end]
    }
}

impl<'a> Parser<'a> for OpenMetricsParser<'a> {
    fn next_entry(&mut self) -> Result<Option<Entry>, ParseError> {
        loop {
            self.line_start = self.lexer.pos;
            self.name = None;
            self.labels.clear();
            if !self.ignore_exemplar {
                self.exemplar = None;
                self.exemplar_labels.clear();
            }
            let entry = match self.lexer.lex() {
                Token::EofWord => {
                    if self.lexer.lex() != Token::Eof {
                        return Err(ParseError::DataAfterEof {
                            offset: self.lexer.token_start,
                        });
                    }
                    return Ok(None);
                }
                Token::Eof => return Err(ParseError::MissingEof),
                kind @ (Token::Help | Token::Type | Token::Unit) => self.parse_metadata(kind)?,
                first @ (Token::BraceOpen | Token::MName) => self.parse_series(first)?,
                _ => return Err(self.unexpected("expected a valid start token")),
            };
            if entry == Entry::Series && self.skip_created_series && self.is_created_series() {
                continue;
            }
            return Ok(Some(entry));
        }
    }

    fn help(&self) -> (&'a [u8], Cow<'a, [u8]>) {
        // OpenMetrics help text uses label value escaping, unlike Prometheus text.
        (self.metric_name(), unescape(self.text))
    }

    fn metric_type(&self) -> (&'a [u8], MetricType) {
        (self.metric_name(), self.metric_type)
    }

    fn unit(&self) -> (&'a [u8], &'a [u8]) {
        (self.metric_name(), self.text)
    }

    fn comment(&self) -> &'a [u8] {
        self.text
    }

    fn series(&self) -> (&'a [u8], Option<i64>, f64) {
        (self.series, self.timestamp, self.value)
    }

    fn labels(&self, buf: &mut LabelsBuffer<'a>) {
        buf.clear();
        buf.push(
            Cow::Borrowed(METRIC_NAME_LABEL),
            unescape_label(self.metric_name()),
        );
        for &(name, value) in &self.labels {
            let name = unescape_label(name);
            let value = normalize_float_label(self.metric_type, &name, unescape_label(value));
            buf.push(name, value);
        }
        buf.sort();
    }

    fn exemplar(&self, buf: &mut LabelsBuffer<'a>) -> Option<(f64, Option<i64>)> {
        buf.clear();
        // Go keeps exemplar labels escaped.
        for &(name, value) in &self.exemplar_labels {
            buf.push(
                String::from_utf8_lossy(name),
                String::from_utf8_lossy(value),
            );
        }
        buf.sort();
        self.exemplar
    }

    /// Peeks ahead for the `_created` line of the current series, like Go's `StartTimestamp`,
    /// including its cache of the last hit.
    fn start_timestamp(&mut self) -> Option<i64> {
        if !requires_start_timestamp(self.metric_type) {
            self.start_timestamp = None;
            return None;
        }
        let family = self.family_name();
        if let Some(cached) = self.start_timestamp
            && cached > 0
            && self
                .series_key(family)
                .eq(self.start_timestamp_key.iter().copied())
        {
            return Some(cached);
        }
        let mut peek = OpenMetricsParser::new(self.lexer.input);
        peek.lexer = self.lexer.clone();
        peek.ignore_exemplar = true;
        peek.metric_type = self.metric_type;
        peek.labels = std::mem::take(&mut self.peek_labels);
        let found = loop {
            if !matches!(peek.next_entry(), Ok(Some(Entry::Series))) {
                break None;
            }
            let Some(peeked_family) = peek.metric_name().strip_suffix(CREATED_SUFFIX) else {
                continue;
            };
            let same_series = peek.series_key(peeked_family).eq(self.series_key(family));
            // Go converts NaN and Inf into a timestamp that looks valid.
            break (same_series && peek.value.is_finite()).then_some((peek.value * 1000.0) as i64);
        };
        self.peek_labels = std::mem::take(&mut peek.labels);
        if found.is_some() {
            let mut key = std::mem::take(&mut self.start_timestamp_key);
            key.clear();
            key.extend(self.series_key(family));
            self.start_timestamp_key = key;
        }
        self.start_timestamp = found;
        found
    }
}

/// Go's `typeRequiresST`.
fn requires_start_timestamp(metric_type: MetricType) -> bool {
    matches!(
        metric_type,
        MetricType::Counter | MetricType::Summary | MetricType::Histogram
    )
}

fn is_float_label(metric_type: MetricType, name: &[u8]) -> bool {
    match metric_type {
        MetricType::Histogram => name == BUCKET_LABEL.as_bytes(),
        MetricType::Summary => name == QUANTILE_LABEL.as_bytes(),
        _ => false,
    }
}

/// Go's `normalizeFloatsInLabelValues`: `le` on histograms and `quantile` on summaries
/// use OpenMetrics float formatting. Keeps `value` borrowed when it is already canonical.
fn normalize_float_label<'v>(
    metric_type: MetricType,
    name: &str,
    value: Cow<'v, str>,
) -> Cow<'v, str> {
    // NOTE: Go's ParseFloat also accepts hex floats here (le="0x1p3"); we leave them as is.
    let Some(parsed) = is_float_label(metric_type, name.as_bytes())
        .then(|| parse_go_float(&value))
        .flatten()
    else {
        return value;
    };
    if let Some(common) = common_openmetrics_float(parsed) {
        return Cow::Borrowed(common);
    }
    let mut formatted = FloatBuf::default();
    if format_openmetrics_float(parsed, &mut formatted).is_err() || formatted.as_str() == value {
        return value;
    }
    Cow::Owned(formatted.as_str().to_owned())
}

/// Maps a `# TYPE` value to a [`MetricType`].
fn parse_metric_type(text: &[u8]) -> Result<MetricType, ParseError> {
    [
        MetricType::Counter,
        MetricType::Gauge,
        MetricType::Histogram,
        MetricType::GaugeHistogram,
        MetricType::Summary,
        MetricType::Info,
        MetricType::Stateset,
        MetricType::Unknown,
    ]
    .into_iter()
    .find(|metric_type| metric_type.as_str().as_bytes() == text)
    .ok_or_else(|| ParseError::InvalidMetricType(String::from_utf8_lossy(text).into_owned()))
}

/// Go's `lvalReplacer`, which replaces `\"`, `\\`, and `\n`.
fn unescape(raw: &[u8]) -> Cow<'_, [u8]> {
    if !raw.contains(&b'\\') {
        return Cow::Borrowed(raw);
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut bytes = raw.iter().copied().peekable();
    while let Some(byte) = bytes.next() {
        let replaced = match (byte, bytes.peek()) {
            (b'\\', Some(b'\\')) => b'\\',
            (b'\\', Some(b'n')) => b'\n',
            (b'\\', Some(b'"')) => b'"',
            _ => {
                out.push(byte);
                continue;
            }
        };
        bytes.next();
        out.push(replaced);
    }
    Cow::Owned(out)
}

/// Unescapes a label name or value. Go does not check that names are UTF-8, so invalid
/// bytes become U+FFFD here instead of failing the parse.
fn unescape_label(raw: &[u8]) -> Cow<'_, str> {
    match unescape(raw) {
        Cow::Borrowed(raw) => String::from_utf8_lossy(raw),
        Cow::Owned(bytes) => Cow::Owned(
            String::from_utf8(bytes)
                .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned()),
        ),
    }
}

/// Go's `parseFloat` in promparse.go, which rejects `pP_` before `strconv.ParseFloat`.
fn parse_sample_value(raw: &[u8]) -> Option<f64> {
    if raw.iter().any(|byte| matches!(byte, b'p' | b'P' | b'_')) {
        return None;
    }
    parse_go_float(std::str::from_utf8(raw).ok()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Result<(), ParseError> {
        let mut parser = OpenMetricsParser::new(input);
        while parser.next_entry()?.is_some() {}
        Ok(())
    }

    #[test]
    fn requires_eof() {
        assert_eq!(parse_all(b"a 1\n"), Err(ParseError::MissingEof));
        assert_eq!(parse_all(b"a 1"), Err(ParseError::MissingEof));
        assert_eq!(parse_all(b"a 1\n# EOF\n"), Ok(()));
        assert_eq!(parse_all(b"a 1\n# EOF"), Ok(()));
    }

    #[test]
    fn rejects_data_after_eof() {
        assert_eq!(
            parse_all(b"# EOF\na 1\n"),
            Err(ParseError::DataAfterEof { offset: 6 })
        );
        assert!(matches!(
            parse_all(b"a 1\n# EOF\n\n"),
            Err(ParseError::DataAfterEof { .. })
        ));
    }

    #[test]
    fn rejects_malformed_exemplars() {
        let inputs: &[&[u8]] = &[
            b"a_total 1 # {id=\"abc\" 1\n# EOF\n",
            b"a_total 1 # {\"id\"} 1\n# EOF\n",
            b"a_total 1 # {id=\"abc\"}\n# EOF\n",
            b"a_total 1 # {id=\"abc\"} 1 NaN\n# EOF\n",
        ];
        for input in inputs {
            assert!(
                matches!(
                    parse_all(input),
                    Err(ParseError::UnexpectedToken { .. } | ParseError::InvalidTimestamp(_))
                ),
                "{:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn rejects_unit_that_is_not_a_suffix() {
        assert!(matches!(
            parse_all(b"# UNIT a_seconds bytes\n# EOF\n"),
            Err(ParseError::InvalidUnit { .. })
        ));
        assert!(matches!(
            parse_all(b"# UNIT seconds seconds\n# EOF\n"),
            Err(ParseError::InvalidUnit { .. })
        ));
        assert_eq!(parse_all(b"# UNIT a_seconds seconds\n# EOF\n"), Ok(()));
    }

    fn first_start_timestamp(input: &'static [u8]) -> Option<i64> {
        let mut parser = OpenMetricsParser::new(input).skip_created_series();
        while parser.next_entry().unwrap() != Some(Entry::Series) {}
        parser.start_timestamp()
    }

    #[test]
    fn start_timestamp_stops_on_invalid_exemplar() {
        // Go's lookahead loops forever on this input.
        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let input = b"# TYPE a counter\na_total 1\na_total{x=\"1\"} 2 # {id=\"x\"}x\n# EOF\n";
            done.send(first_start_timestamp(input)).unwrap();
        });
        let start = result.recv_timeout(std::time::Duration::from_secs(5));
        assert_eq!(start, Ok(None));
    }

    #[test]
    fn start_timestamp_ignores_non_finite_created() {
        for input in [
            &b"# TYPE a counter\na_total 1\na_created NaN\n# EOF\n"[..],
            b"# TYPE a counter\na_total 1\na_created +Inf\n# EOF\n",
        ] {
            let input: &'static [u8] = input;
            assert_eq!(first_start_timestamp(input), None);
        }
    }

    #[test]
    fn label_value_drops_nul_after_closing_quote() {
        let mut parser = OpenMetricsParser::new(b"a{b=\"c\"\0} 1\n# EOF\n");
        let mut buf = LabelsBuffer::new();
        assert_eq!(parser.next_entry(), Ok(Some(Entry::Series)));
        parser.labels(&mut buf);
        assert_eq!(buf.get("b"), Some("c"));
    }
}
