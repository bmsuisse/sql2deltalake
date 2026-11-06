//! The query builder against the SQL the Python/sqlglot implementation generated
//! (`scripts/gen_sql_golden.py` wrote `golden/sqlgen.json`).

use serde_json::Value;
use sql2deltalake_core::scalar::Scalar;
use sql2deltalake_core::sqlgen::*;

fn column(v: &Value) -> Column {
    Column {
        name: v["name"].as_str().unwrap().into(),
        target: v["target"].as_str().unwrap().into(),
        type_key: v["type_key"].as_str().unwrap().into(),
        type_sql: v["type_sql"].as_str().unwrap().into(),
        orig_type: v["orig_type"].as_str().unwrap().into(),
        is_string: v["is_string"].as_bool().unwrap(),
        is_numeric: v["is_numeric"].as_bool().unwrap(),
        is_identity: v["is_identity"].as_bool().unwrap(),
    }
}

fn scalar(v: &Value) -> Scalar {
    let (k, v) = v.as_object().unwrap().iter().next().unwrap();
    match k.as_str() {
        "bool" => Scalar::Bool(v.as_bool().unwrap()),
        "int" => Scalar::Int(v.as_i64().unwrap()),
        "float" => Scalar::Float(v.as_f64().unwrap()),
        "decimal" => {
            Scalar::Decimal { value: v[0].as_str().unwrap().parse().unwrap(), scale: v[1].as_i64().unwrap() as i8 }
        }
        "str" => Scalar::Str(v.as_str().unwrap().into()),
        "bytes" => {
            let h = v.as_str().unwrap();
            Scalar::Bytes((0..h.len()).step_by(2).map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap()).collect())
        }
        "timestamp" => Scalar::Timestamp { micros: v[0].as_i64().unwrap(), utc: v[1].as_bool().unwrap() },
        "date" => Scalar::Date(v.as_i64().unwrap() as i32),
        other => panic!("{other}"),
    }
}

#[test]
fn sql_matches_sqlglot() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("golden/sqlgen.json")).unwrap();
    let mut checked = 0;
    for case in &cases {
        let dialect = Dialect::parse(case["dialect"].as_str().unwrap()).unwrap();
        let source = match &case["source"] {
            Value::String(q) => SourceRef::Query(q.clone()),
            Value::Array(parts) => SourceRef::Table(parts.iter().map(|p| p.as_str().unwrap().to_string()).collect()),
            _ => panic!(),
        };
        let type_map: TypeMap = case["type_map"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                (k.clone(), MappedType { sql: v[0].as_str().unwrap().into(), is_string: v[1].as_bool().unwrap() })
            })
            .collect();
        let sql = SourceSql { dialect, source, type_map, no_trim: case["no_trim"].as_bool().unwrap() };
        let cols: Vec<Column> = case["columns"].as_array().unwrap().iter().map(column).collect();
        let all: Vec<&Column> = cols.iter().collect();
        let exp = &case["expected"];
        let lit = load_timestamp_literal(dialect, case["now_micros"].as_i64().unwrap());
        assert_eq!(lit, exp["load_timestamp"].as_str().unwrap());
        let vf = ValidFrom::Literal(lit);
        assert_eq!(sql.full_load(&all, &ValidFrom::SourceClock), exp["full_load_clock"].as_str().unwrap());
        assert_eq!(sql.full_load(&all, &vf), exp["full_load_literal"].as_str().unwrap());
        let delta = cols.last().unwrap();
        assert_eq!(sql.pk_ts(&[&cols[0], &cols[4], delta]), exp["pk_ts"].as_str().unwrap());
        assert_eq!(sql.source_ts_cnt(Some(delta)), exp["source_ts_cnt"].as_str().unwrap());
        assert_eq!(sql.updates(&all, &[], &ValidFrom::SourceClock), exp["updates_no_criterion"].as_str().unwrap());
        for u in exp["updates"].as_array().unwrap() {
            let c = &cols[u["column"].as_u64().unwrap() as usize];
            let crit = sql.criterion(c, u["op"].as_str().unwrap(), &scalar(&u["value"]));
            assert_eq!(sql.updates(&all, &[crit], &vf), u["sql"].as_str().unwrap(), "value {}", u["value"]);
            checked += 1;
        }
        for r in exp["rows_by_pk"].as_array().unwrap() {
            let pks: Vec<&Column> =
                r["pks"].as_array().unwrap().iter().map(|i| &cols[i.as_u64().unwrap() as usize]).collect();
            assert_eq!(sql.rows_by_pk_json(&all, &pks, r["json"].as_str().unwrap(), &vf), r["sql"].as_str().unwrap());
        }
        checked += 1;
    }
    assert!(checked > 30);
}
