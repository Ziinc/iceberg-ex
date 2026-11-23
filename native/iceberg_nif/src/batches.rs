//! Arrow IPC streams in and out, and incoming batches fitted to a table's schema.

use std::io::Cursor;

use arrow_array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;

use crate::error::Error;

pub fn from_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>, Error> {
    let invalid =
        |e: arrow_schema::ArrowError| Error::invalid(format!("invalid Arrow IPC stream: {e}"));
    StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(invalid)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)
}

pub fn to_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>, Error> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    Ok(writer.into_inner()?)
}

/// `batch` with `target`'s columns, matched by name and cast to their types.
/// A missing nullable column is all nulls; an unknown column is an error.
pub fn conform(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, Error> {
    let input = batch.schema();
    if let Some(extra) = input
        .fields()
        .iter()
        .find(|f| target.field_with_name(f.name()).is_err())
    {
        return Err(Error::invalid(format!("unknown column {}", extra.name())));
    }
    // Overflowing or unparseable values fail the write rather than becoming nulls.
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };

    let columns = target
        .fields()
        .iter()
        .map(|field| match input.index_of(field.name()) {
            Ok(i) if batch.column(i).data_type() == field.data_type() => {
                Ok(batch.column(i).clone())
            }
            Ok(i) => cast_with_options(batch.column(i), field.data_type(), &options)
                .map_err(|e| Error::invalid(format!("column {}: {e}", field.name()))),
            Err(_) if field.is_nullable() => {
                Ok(new_null_array(field.data_type(), batch.num_rows()))
            }
            Err(_) => Err(Error::invalid(format!(
                "missing required column {}",
                field.name()
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &options,
    )?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, Int64Array, LargeStringArray, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn target() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(fields: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        RecordBatch::try_from_iter(fields.into_iter().map(|(n, a)| (n.to_string(), a))).unwrap()
    }

    #[test]
    fn casts_matching_columns_and_fills_missing_nullable_ones() {
        let input = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2])))]);
        let out = conform(&input, &target()).unwrap();
        assert_eq!(out.schema(), target());
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
        assert_eq!(out.column(1).null_count(), 2);

        let input = batch(vec![
            ("name", Arc::new(LargeStringArray::from(vec!["a"]))),
            ("id", Arc::new(Int32Array::from(vec![1]))),
        ]);
        let out = conform(&input, &target()).unwrap();
        assert_eq!(
            out.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
    }

    #[test]
    fn refuses_unknown_missing_required_and_overflowing_columns() {
        let unknown = batch(vec![
            ("id", Arc::new(Int32Array::from(vec![1]))),
            ("x", Arc::new(Int32Array::from(vec![1]))),
        ]);
        assert_eq!(
            conform(&unknown, &target()),
            Err(Error::invalid("unknown column x"))
        );

        let missing = batch(vec![("name", Arc::new(StringArray::from(vec!["a"])))]);
        assert_eq!(
            conform(&missing, &target()),
            Err(Error::invalid("missing required column id"))
        );

        let overflow = batch(vec![("id", Arc::new(Int64Array::from(vec![i64::MAX])))]);
        assert!(conform(&overflow, &target()).is_err());

        let null_id = batch(vec![("id", Arc::new(Int32Array::from(vec![None])))]);
        assert!(conform(&null_id, &target()).is_err());
    }

    #[test]
    fn ipc_round_trips_and_rejects_garbage() {
        let input = batch(vec![("id", Arc::new(Int32Array::from(vec![1, 2])))]);
        let bytes = to_ipc(&input.schema(), std::slice::from_ref(&input)).unwrap();
        assert_eq!(from_ipc(&bytes).unwrap(), vec![input]);
        assert!(from_ipc(b"not arrow").is_err());
    }
}
