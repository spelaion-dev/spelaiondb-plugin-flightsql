//! Flight SQL's results are Arrow already: each column is converted to the Arrow type of its
//! SpelaionDB logical type (the plugin converts, never the core), as cheaply as Arrow's own
//! casts allow.
//!
//! - booleans are `Bool`; signed integers and unsigned up to 32 bits are `Int` (64-bit);
//!   `UInt64` does not fit and is `Decimal`, written as text;
//! - floats are `Float` (64-bit); decimals are `Decimal`, written as text;
//! - strings are `Text`; binaries are `Bytes`;
//! - `Date32`/`Date64` are `Date`; times are `Time` (microseconds); timestamps are `Timestamp`,
//!   or `TimestampTz` (in UTC) when they carry a time zone (microseconds: finer ones lose their
//!   last digits);
//! - dictionaries are their values' type;
//! - everything else (lists, structs, maps, intervals, durations…) is `Native`, written as
//!   Arrow writes the value.
//!
//! The native type is the server's SQL type name when it gives one in the field's metadata
//! (`ARROW:FLIGHT:SQL:TYPE_NAME`), and Arrow's type otherwise.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringBuilder};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// The logical type a column of `data_type` becomes.
pub fn logical(data_type: &DataType) -> LogicalType {
    match data_type {
        DataType::Boolean => LogicalType::Bool,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => LogicalType::Int,
        DataType::UInt64 => LogicalType::Decimal,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => LogicalType::Float,
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => LogicalType::Decimal,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => LogicalType::Text,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => LogicalType::Bytes,
        DataType::Date32 | DataType::Date64 => LogicalType::Date,
        DataType::Time32(_) | DataType::Time64(_) => LogicalType::Time,
        DataType::Timestamp(_, None) => LogicalType::Timestamp,
        DataType::Timestamp(_, Some(_)) => LogicalType::TimestampTz,
        DataType::Dictionary(_, values) => logical(values),
        _ => LogicalType::Native,
    }
}

/// The name shown for a field's type.
fn native_type(field: &Field) -> String {
    field
        .metadata()
        .get("ARROW:FLIGHT:SQL:TYPE_NAME")
        .cloned()
        .unwrap_or_else(|| field.data_type().to_string())
}

/// SpelaionDB's schema for a result whose Arrow schema is `schema`.
pub fn result_schema(schema: &Schema) -> Arc<ResultSchema> {
    Arc::new(ResultSchema::new(
        schema
            .fields()
            .iter()
            .map(|field| ColumnMeta {
                name: field.name().clone(),
                logical: logical(field.data_type()),
                native_type: native_type(field),
                native_id: None,
                origin: None,
            })
            .collect(),
    ))
}

/// `batch` with each column converted to its logical type's Arrow type.
pub fn convert(batch: &RecordBatch, schema: &Arc<ResultSchema>) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(&schema.columns)
        .map(|(array, column)| convert_array(array, column.logical))
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(schema.arrow.clone(), columns)
        .map_err(|e| DriverError::Other(format!("building a batch: {e}")))
}

fn convert_array(array: &ArrayRef, logical: LogicalType) -> Result<ArrayRef> {
    let target = logical.arrow_type();
    if array.data_type() == &target {
        return Ok(array.clone());
    }
    let array = match array.data_type() {
        // Its values, then as those.
        DataType::Dictionary(_, values) => cast(array, values)?,
        _ => array.clone(),
    };
    match logical {
        LogicalType::Native => text(&array),
        // A time zone is the instant's: in UTC.
        LogicalType::TimestampTz => cast(
            &array,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        _ => match cast(&array, &target) {
            Ok(converted) => Ok(converted),
            // What Arrow cannot cast to text is written as Arrow writes it.
            Err(_) if target == DataType::Utf8 => text(&array),
            Err(e) => Err(e),
        },
    }
}

fn cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    cast_with_options(array, to, &options).map_err(|e| DriverError::Decode {
        type_name: array.data_type().to_string(),
        message: e.to_string(),
    })
}

/// Each value as Arrow writes it; NULL stays NULL.
fn text(array: &ArrayRef) -> Result<ArrayRef> {
    let options = FormatOptions::default();
    let formatter =
        ArrayFormatter::try_new(array.as_ref(), &options).map_err(|e| DriverError::Decode {
            type_name: array.data_type().to_string(),
            message: e.to_string(),
        })?;
    let mut out = StringBuilder::with_capacity(array.len(), array.len() * 8);
    for i in 0..array.len() {
        if array.is_null(i) {
            out.append_null();
        } else {
            out.append_value(formatter.value(i).to_string());
        }
    }
    Ok(Arc::new(out.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Date64Array, Decimal128Array, DictionaryArray, Int8Array, ListArray, StringArray,
        TimestampNanosecondArray, UInt64Array,
    };
    use arrow::datatypes::{Int8Type, Int32Type};
    use spelaion_driver_api::Value;

    fn one(array: ArrayRef) -> (LogicalType, Value) {
        let field = Field::new("c", array.data_type().clone(), true);
        let schema = result_schema(&Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "c",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        let converted = convert(&batch, &schema).unwrap();
        let logical = schema.columns[0].logical;
        (
            logical,
            Value::from_array(converted.column(0), 0, logical).unwrap(),
        )
    }

    #[test]
    fn columns_become_their_logical_types() {
        assert_eq!(
            one(Arc::new(Int8Array::from(vec![-3]))),
            (LogicalType::Int, Value::Int(-3))
        );
        assert_eq!(
            one(Arc::new(UInt64Array::from(vec![u64::MAX]))),
            (
                LogicalType::Decimal,
                Value::Decimal("18446744073709551615".into())
            )
        );
        let decimal = Decimal128Array::from(vec![-1234])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            one(Arc::new(decimal)),
            (LogicalType::Decimal, Value::Decimal("-12.34".into()))
        );
        assert_eq!(
            one(Arc::new(Date64Array::from(vec![86_400_000]))),
            (LogicalType::Date, Value::Date(1))
        );
        let ts = TimestampNanosecondArray::from(vec![1_500_000_000]).with_timezone("+02:00");
        assert_eq!(
            one(Arc::new(ts)),
            (LogicalType::TimestampTz, Value::TimestampTz(1_500_000))
        );
        let dict: DictionaryArray<Int8Type> = vec!["a", "b", "a"].into_iter().collect();
        assert_eq!(
            one(Arc::new(dict)),
            (LogicalType::Text, Value::Text("a".into()))
        );
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(3),
        ])]);
        assert_eq!(
            one(Arc::new(list)),
            (LogicalType::Native, Value::Native("[1, , 3]".into()))
        );
        let text = StringArray::from(vec![Some("x")]);
        assert_eq!(
            one(Arc::new(text)),
            (LogicalType::Text, Value::Text("x".into()))
        );
    }

    #[test]
    fn the_servers_type_name_is_shown_when_it_gives_one() {
        let field = Field::new("c", DataType::Int32, true).with_metadata(
            std::collections::HashMap::from([(
                "ARROW:FLIGHT:SQL:TYPE_NAME".to_string(),
                "INTEGER".to_string(),
            )]),
        );
        assert_eq!(native_type(&field), "INTEGER");
        assert_eq!(
            native_type(&Field::new("c", DataType::Int32, true)),
            "Int32"
        );
    }
}
