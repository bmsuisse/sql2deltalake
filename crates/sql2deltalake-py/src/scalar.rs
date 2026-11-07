//! Python values <-> core scalars.

use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyInt, PyString};
use sql2deltalake_core::scalar::Scalar;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    // Howard Hinnant's algorithm: days since 1970-01-01
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A Python value as a core scalar: None, bool, int, float, str, bytes, Decimal, date, datetime.
pub fn to_scalar(v: &Bound<'_, PyAny>) -> PyResult<Scalar> {
    if v.is_none() {
        return Ok(Scalar::Null);
    }
    if v.is_instance_of::<PyBool>() {
        return Ok(Scalar::Bool(v.extract()?));
    }
    if v.is_instance_of::<PyInt>() {
        return Ok(Scalar::Int(v.extract()?));
    }
    if v.is_instance_of::<PyFloat>() {
        return Ok(Scalar::Float(v.extract()?));
    }
    if v.is_instance_of::<PyString>() {
        return Ok(Scalar::Str(v.extract()?));
    }
    if v.is_instance_of::<PyBytes>() || v.get_type().name()? == "bytearray" {
        return Ok(Scalar::Bytes(v.extract()?));
    }
    let type_name = v.get_type().name()?.to_string();
    match type_name.as_str() {
        "Decimal" => {
            let t = v.call_method0("as_tuple")?;
            let sign: i64 = t.getattr("sign")?.extract()?;
            let digits: Vec<u8> = t.getattr("digits")?.extract()?;
            let exp: i64 = t.getattr("exponent")?.extract()?;
            // Decimal128: at most 38 digits, the scale within [-38, 38]
            let out_of_range = || PyOverflowError::new_err(format!("{v} does not fit a Decimal128 (38 digits)"));
            let scale = i8::try_from(-exp).ok().filter(|s| (-38..=38).contains(s)).ok_or_else(out_of_range)?;
            if digits.len() > 38 {
                return Err(out_of_range());
            }
            let value = digits.iter().fold(0i128, |acc, d| acc * 10 + i128::from(*d));
            Ok(Scalar::Decimal { value: if sign == 1 { -value } else { value }, scale })
        }
        "date" => {
            let (y, m, d): (i64, i64, i64) =
                (v.getattr("year")?.extract()?, v.getattr("month")?.extract()?, v.getattr("day")?.extract()?);
            Ok(Scalar::Date(days_from_civil(y, m, d) as i32))
        }
        "datetime" | "Timestamp" => {
            let utc = !v.getattr("tzinfo")?.is_none();
            let v = if utc {
                let tz = v.py().import("datetime")?.getattr("timezone")?.getattr("utc")?;
                v.call_method1("astimezone", (tz,))?
            } else {
                v.clone()
            };
            let g = |n: &str| -> PyResult<i64> { v.getattr(n)?.extract() };
            let days = days_from_civil(g("year")?, g("month")?, g("day")?);
            let secs = days * 86_400 + g("hour")? * 3600 + g("minute")? * 60 + g("second")?;
            Ok(Scalar::Timestamp { micros: secs * 1_000_000 + g("microsecond")?, utc })
        }
        other => Err(PyTypeError::new_err(format!("unsupported value type {other}"))),
    }
}
