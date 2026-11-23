//! Arrow IPC streams in and out, and incoming batches fitted to a table's schema.

use std::collections::HashSet;
use std::io::Cursor;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{new_null_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, SchemaRef};

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
    let mut seen = HashSet::new();
    for field in input.fields() {
        if !seen.insert(field.name()) {
            return Err(Error::invalid(format!("duplicate column {}", field.name())));
        }
        if target.field_with_name(field.name()).is_err() {
            return Err(Error::invalid(format!("unknown column {}", field.name())));
        }
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
            Ok(i) => {
                let column = batch.column(i);
                let cast = cast_with_options(column, field.data_type(), &options)
                    .map_err(|e| Error::invalid(format!("column {}: {e}", field.name())))?;
                if column.data_type().is_floating() && !field.data_type().is_floating() {
                    exact(column, &cast, field.name())?;
                }
                Ok(cast)
            }
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

/// Fails if casting floats to `cast`'s type (integer or decimal) changed a value.
fn exact(floats: &ArrayRef, cast: &ArrayRef, column: &str) -> Result<(), Error> {
    let before = cast_with_options(floats, &DataType::Float64, &CastOptions::default())?;
    let after = cast_with_options(cast, &DataType::Float64, &CastOptions::default())?;
    let changed = before
        .as_primitive::<Float64Type>()
        .iter()
        .zip(after.as_primitive::<Float64Type>().iter())
        .find_map(|(b, a)| match (b, a) {
            (Some(b), Some(a)) if a != b => Some(b),
            _ => None,
        });
    match changed {
        Some(value) => Err(Error::invalid(format!(
            "column {column}: {value} cannot be stored without losing precision"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray};
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
    fn refuses_duplicate_input_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let ids: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1]));
        let input = RecordBatch::try_new(schema, vec![ids.clone(), ids]).unwrap();
        assert_eq!(
            conform(&input, &target()),
            Err(Error::invalid("duplicate column id"))
        );
    }

    #[test]
    fn casts_floats_to_integers_only_when_exact() {
        let exact = batch(vec![(
            "id",
            Arc::new(Float64Array::from(vec![Some(2.0), None])),
        )]);
        let out = conform(&exact, &target());
        assert!(out.is_err(), "a null in the required id column");

        let target = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let out = conform(&exact, &target).unwrap();
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), None]
        );

        let lossy = batch(vec![("id", Arc::new(Float64Array::from(vec![1.0, 1.5])))]);
        assert_eq!(
            conform(&lossy, &target),
            Err(Error::invalid(
                "column id: 1.5 cannot be stored without losing precision"
            ))
        );
    }

    #[test]
    fn ipc_round_trips_and_rejects_garbage() {
        let input = batch(vec![("id", Arc::new(Int32Array::from(vec![1, 2])))]);
        let bytes = to_ipc(&input.schema(), std::slice::from_ref(&input)).unwrap();
        assert_eq!(from_ipc(&bytes).unwrap(), vec![input]);
        assert!(from_ipc(b"not arrow").is_err());
    }
}
