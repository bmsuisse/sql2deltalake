//! Single values read from Arrow arrays (delta values, counts, source clock), rendered the way
//! the Python implementation rendered the equivalent Python objects.

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate};

use crate::error::{Error, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal {
        value: i128,
        scale: i8,
    },
    Str(String),
    Bytes(Vec<u8>),
    /// Days since 1970-01-01.
    Date(i32),
    /// Microseconds since the epoch; `utc` for time-zone aware values (always UTC here).
    Timestamp {
        micros: i64,
        utc: bool,
    },
}

impl Scalar {
    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Scalar::Int(v) => Some(*v),
            // rowversion read from the source; the local copy holds CAST(rowversion AS BIGINT)
            Scalar::Bytes(b) if b.len() <= 8 => Some(b.iter().fold(0i64, |acc, x| (acc << 8) | i64::from(*x))),
            _ => None,
        }
    }

    /// Python's `str()` of the equivalent value (`datetime.isoformat(sep=" ")` for timestamps).
    pub fn to_text(&self) -> String {
        match self {
            Scalar::Null => "None".into(),
            Scalar::Bool(v) => if *v { "True" } else { "False" }.into(),
            Scalar::Int(v) => v.to_string(),
            Scalar::Float(v) => python_float_repr(*v),
            Scalar::Decimal { value, scale } => decimal_text(*value, *scale),
            Scalar::Str(s) => s.clone(),
            Scalar::Bytes(b) => {
                let mut s = String::from("b'");
                for x in b {
                    s.push_str(&format!("\\x{x:02x}"));
                }
                s.push('\'');
                s
            }
            Scalar::Date(days) => date_text(*days),
            Scalar::Timestamp { micros, utc } => {
                let dt = DateTime::from_timestamp_micros(*micros).map(|d| d.naive_utc());
                let Some(dt) = dt else { return micros.to_string() };
                let mut s = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                let frac = micros.rem_euclid(1_000_000);
                if frac != 0 {
                    s.push_str(&format!(".{frac:06}"));
                }
                if *utc {
                    s.push_str("+00:00");
                }
                s
            }
        }
    }

    /// `%Y-%m-%d %H:%M:%S.%f`: always six fractional digits.
    pub fn to_text_fixed_micros(&self) -> String {
        match self {
            Scalar::Timestamp { micros, .. } => match DateTime::from_timestamp_micros(*micros) {
                Some(d) => format!("{}.{:06}", d.naive_utc().format("%Y-%m-%d %H:%M:%S"), micros.rem_euclid(1_000_000)),
                None => micros.to_string(),
            },
            other => other.to_text(),
        }
    }

    /// `json.dumps(value, default=str)`.
    pub fn to_json(&self) -> String {
        match self {
            Scalar::Null => "null".into(),
            Scalar::Bool(v) => if *v { "true" } else { "false" }.into(),
            Scalar::Int(v) => v.to_string(),
            Scalar::Float(v) => python_float_repr(*v),
            other => json_string(&other.to_text()),
        }
    }
}

/// A JSON string as Python's `json.dumps` writes it (`ensure_ascii=True`).
pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) >= 0x7f => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Python's `repr(float)`: shortest round trip, exponent below 1e-4 and from 1e16.
pub fn python_float_repr(v: f64) -> String {
    if v.is_nan() {
        return "nan".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0".into() } else { "0.0".into() };
    }
    // the decimal exponent of the shortest round-trip digits, as Python decides by it (a float
    // log10 rounds up just below a power of ten)
    let s = format!("{v:e}");
    let (mantissa, e) = s.split_once('e').unwrap_or((&s, "0"));
    let e: i32 = e.parse().unwrap_or(0);
    if (-4..16).contains(&e) {
        let s = format!("{v}");
        if s.contains('.') { s } else { format!("{s}.0") }
    } else {
        format!("{mantissa}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs())
    }
}

fn decimal_text(value: i128, scale: i8) -> String {
    if scale <= 0 {
        let mut s = value.to_string();
        s.push_str(&"0".repeat((-scale) as usize));
        return s;
    }
    let neg = value < 0;
    let digits = value.unsigned_abs().to_string();
    let scale = scale as usize;
    let digits =
        if digits.len() <= scale { format!("{}{digits}", "0".repeat(scale - digits.len() + 1)) } else { digits };
    let (int, frac) = digits.split_at(digits.len() - scale);
    format!("{}{int}.{frac}", if neg { "-" } else { "" })
}

fn date_text(days: i32) -> String {
    days.checked_add(719_163)
        .and_then(NaiveDate::from_num_days_from_ce_opt)
        .map_or_else(|| days.to_string(), |d| d.format("%Y-%m-%d").to_string())
}

fn ts_micros(v: i64, unit: &TimeUnit) -> Result<i64> {
    let factor = match unit {
        TimeUnit::Second => 1_000_000,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => return Ok(v),
        TimeUnit::Nanosecond => return Ok(v.div_euclid(1_000)),
    };
    v.checked_mul(factor)
        .ok_or_else(|| Error::msg(format!("timestamp {v} ({unit:?}) is outside the range of microseconds")))
}

/// The value at `idx` of `array`.
pub fn scalar_at(array: &ArrayRef, idx: usize) -> Result<Scalar> {
    if array.is_null(idx) {
        return Ok(Scalar::Null);
    }
    Ok(match array.data_type() {
        DataType::Null => Scalar::Null,
        DataType::Boolean => Scalar::Bool(array.as_boolean().value(idx)),
        DataType::Int8 => Scalar::Int(array.as_primitive::<Int8Type>().value(idx).into()),
        DataType::Int16 => Scalar::Int(array.as_primitive::<Int16Type>().value(idx).into()),
        DataType::Int32 => Scalar::Int(array.as_primitive::<Int32Type>().value(idx).into()),
        DataType::Int64 => Scalar::Int(array.as_primitive::<Int64Type>().value(idx)),
        DataType::UInt8 => Scalar::Int(array.as_primitive::<UInt8Type>().value(idx).into()),
        DataType::UInt16 => Scalar::Int(array.as_primitive::<UInt16Type>().value(idx).into()),
        DataType::UInt32 => Scalar::Int(array.as_primitive::<UInt32Type>().value(idx).into()),
        DataType::UInt64 => {
            let v = array.as_primitive::<UInt64Type>().value(idx);
            Scalar::Int(
                i64::try_from(v).map_err(|_| Error::msg(format!("UInt64 value {v} is above the BIGINT range")))?,
            )
        }
        DataType::Float32 => Scalar::Float(array.as_primitive::<Float32Type>().value(idx).into()),
        DataType::Float64 => Scalar::Float(array.as_primitive::<Float64Type>().value(idx)),
        DataType::Decimal128(_, s) => {
            Scalar::Decimal { value: array.as_primitive::<Decimal128Type>().value(idx), scale: *s }
        }
        DataType::Utf8 => Scalar::Str(array.as_string::<i32>().value(idx).to_string()),
        DataType::LargeUtf8 => Scalar::Str(array.as_string::<i64>().value(idx).to_string()),
        DataType::Utf8View => Scalar::Str(array.as_string_view().value(idx).to_string()),
        DataType::Binary => Scalar::Bytes(array.as_binary::<i32>().value(idx).to_vec()),
        DataType::LargeBinary => Scalar::Bytes(array.as_binary::<i64>().value(idx).to_vec()),
        DataType::FixedSizeBinary(_) => Scalar::Bytes(array.as_fixed_size_binary().value(idx).to_vec()),
        DataType::Date32 => Scalar::Date(array.as_primitive::<Date32Type>().value(idx)),
        DataType::Date64 => {
            let ms = array.as_primitive::<Date64Type>().value(idx);
            Scalar::Date(
                i32::try_from(ms.div_euclid(86_400_000))
                    .map_err(|_| Error::msg(format!("Date64 value {ms} is outside the range of dates")))?,
            )
        }
        DataType::Timestamp(unit, tz) => {
            let v = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(idx),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(idx),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(idx),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(idx),
            };
            Scalar::Timestamp { micros: ts_micros(v, unit)?, utc: tz.is_some() }
        }
        other => return Err(Error::msg(format!("unsupported type for a scalar value: {other}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint64_above_i64_is_an_error_not_negative() {
        let a: ArrayRef = std::sync::Arc::new(arrow_array::UInt64Array::from(vec![7, u64::MAX]));
        assert_eq!(scalar_at(&a, 0).unwrap(), Scalar::Int(7));
        assert!(scalar_at(&a, 1).is_err());
    }

    #[test]
    fn out_of_range_times_are_errors_not_wrapped() {
        let a: ArrayRef = std::sync::Arc::new(arrow_array::TimestampSecondArray::from(vec![1, i64::MAX]));
        assert_eq!(scalar_at(&a, 0).unwrap(), Scalar::Timestamp { micros: 1_000_000, utc: false });
        assert!(scalar_at(&a, 1).is_err());
        let a: ArrayRef = std::sync::Arc::new(arrow_array::TimestampMillisecondArray::from(vec![i64::MIN]));
        assert!(scalar_at(&a, 0).is_err());
        let a: ArrayRef = std::sync::Arc::new(arrow_array::Date64Array::from(vec![i64::MAX]));
        assert!(scalar_at(&a, 0).is_err());
        assert_eq!(Scalar::Date(i32::MAX).to_text(), "2147483647");
        assert_eq!(Scalar::Date(i32::MIN).to_text(), "-2147483648");
    }

    #[test]
    fn float_repr_matches_python() {
        assert_eq!(python_float_repr(1.5), "1.5");
        assert_eq!(python_float_repr(3.0), "3.0");
        assert_eq!(python_float_repr(1e-5), "1e-05");
        assert_eq!(python_float_repr(1e16), "1e+16");
        assert_eq!(python_float_repr(0.0001), "0.0001");
    }

    #[test]
    fn decimal_text_keeps_scale() {
        assert_eq!(decimal_text(12500, 3), "12.500");
        assert_eq!(decimal_text(-5, 2), "-0.05");
        assert_eq!(decimal_text(0, 3), "0.000");
    }

    #[test]
    fn timestamp_text_like_isoformat() {
        let t = Scalar::Timestamp { micros: 1_577_934_245_123_456, utc: false };
        assert_eq!(t.to_text(), "2020-01-02 03:04:05.123456");
        let t = Scalar::Timestamp { micros: 1_577_934_245_000_000, utc: true };
        assert_eq!(t.to_text(), "2020-01-02 03:04:05+00:00");
        assert_eq!(t.to_text_fixed_micros(), "2020-01-02 03:04:05.000000");
        assert_eq!(Scalar::Date(18263).to_text(), "2020-01-02");
    }

    #[test]
    fn json_like_python() {
        assert_eq!(json_string("a\"b\u{e9}"), "\"a\\\"b\\u00e9\"");
        assert_eq!(Scalar::Bytes(vec![0, 1]).as_i64(), Some(1));
    }

    /// `json.dumps(s)` of Python 3.
    #[test]
    fn json_string_golden() {
        for (s, python) in [
            ("a\u{7f}b", r#""a\u007fb""#),
            ("\u{1f} ~\u{7f}\u{80}", r#""\u001f ~\u007f\u0080""#),
            ("\u{20ac}\u{1f600}", r#""\u20ac\ud83d\ude00""#),
        ] {
            assert_eq!(json_string(s), python, "{s:?}");
        }
    }

    /// `repr(v)` of Python 3, also near the powers of ten where the notation switches.
    #[test]
    fn float_repr_golden() {
        for (v, python) in [
            (9999999999999998.0, "9999999999999998.0"),
            (-9999999999999998.0, "-9999999999999998.0"),
            (9.999999999999999e-05, "9.999999999999999e-05"),
            (1e16, "1e+16"),
            (1e-5, "1e-05"),
            (0.0001, "0.0001"),
            (1e15, "1000000000000000.0"),
            (1.0000000000000002e16, "1.0000000000000002e+16"),
            (0.00010000000000000002, "0.00010000000000000002"),
            (5e-324, "5e-324"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (-1.5e-7, "-1.5e-07"),
            (12345.678, "12345.678"),
            (0.1, "0.1"),
            (99999.99999999999, "99999.99999999999"),
            (1e22, "1e+22"),
            (123456789012345680.0, "1.2345678901234568e+17"),
        ] {
            assert_eq!(python_float_repr(v), python, "{v:e}");
        }
    }
}
