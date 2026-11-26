//! Arrow IPC streams in and out, columns of Elixir values in, and incoming
//! batches fitted to a table's schema.

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{
    new_null_array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch,
    RecordBatchOptions,
};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rustler::types::map::MapIterator;
use rustler::{Binary, Term, TermType};

use crate::error::Error;

/// Rows a batch may claim beyond 8 per byte of input: columns that carry
/// no buffers (all-null, run-end encoded) still cost memory once cast.
const SPARSE_ROWS: u64 = 1 << 20;

/// Decodes an Arrow IPC stream from untrusted bytes. arrow-ipc allocates
/// what a message's header claims and panics on some malformed buffers, so
/// lengths are checked against the input first and panics become errors.
pub fn from_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>, Error> {
    let invalid =
        |m: &dyn std::fmt::Display| Error::invalid(format!("invalid Arrow IPC stream: {m}"));
    check_framing(bytes).map_err(|m| invalid(&m))?;

    let decode = || -> Result<Vec<RecordBatch>, arrow_schema::ArrowError> {
        StreamReader::try_new(Cursor::new(bytes), None)?.collect()
    };
    let batches = match std::panic::catch_unwind(decode) {
        Ok(result) => result.map_err(|e| invalid(&e))?,
        Err(panic) => {
            let message = (panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "malformed input".into());
            return Err(invalid(&message));
        }
    };

    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if rows > (bytes.len() as u64 * 8).max(SPARSE_ROWS) {
        return Err(invalid(&format!(
            "{rows} rows cannot be in {} bytes",
            bytes.len()
        )));
    }
    Ok(batches)
}

/// Walks the stream's messages (`[0xFFFFFFFF] <metadata length> <metadata> <body>`)
/// and checks every declared length against the bytes that follow it.
fn check_framing(bytes: &[u8]) -> Result<(), String> {
    let longer = || "a message is longer than the input".to_string();
    let mut rest = bytes;
    loop {
        let Some(mut len) = take_i32(&mut rest) else {
            return Ok(());
        };
        if len == -1 {
            len = take_i32(&mut rest).ok_or_else(longer)?;
        }
        if len == 0 {
            return Ok(());
        }
        let len = usize::try_from(len).map_err(|_| longer())?;
        let (meta, tail) = rest.split_at_checked(len).ok_or_else(longer)?;
        let message = arrow_ipc::root_as_message(meta).map_err(|e| format!("{e}"))?;
        let body = usize::try_from(message.bodyLength()).map_err(|_| longer())?;
        rest = tail.get(body..).ok_or_else(longer)?;
    }
}

fn take_i32(rest: &mut &[u8]) -> Option<i32> {
    let (head, tail) = rest.split_first_chunk::<4>()?;
    *rest = tail;
    Some(i32::from_le_bytes(*head))
}

/// A batch from `%{name => values}`: each list one column, all the same
/// length, of binaries (UTF-8), integers, floats (integers allowed among
/// them), booleans or nil. `conform/2` then casts each to the table's type.
pub fn from_columns(term: Term) -> Result<RecordBatch, Error> {
    let columns = MapIterator::new(term).ok_or_else(|| Error::invalid("columns must be a map"))?;
    let (mut fields, mut arrays, mut rows) = (Vec::new(), Vec::new(), None);

    for (key, values) in columns {
        let name: String = key
            .decode()
            .map_err(|_| Error::invalid("column names must be strings"))?;
        let values: Vec<Term> = values
            .decode()
            .map_err(|_| Error::invalid(format!("column {name}: values must be a list")))?;
        match rows {
            Some(n) if n != values.len() => {
                return Err(Error::invalid(format!(
                    "column {name} has {} values, another has {n}",
                    values.len()
                )))
            }
            _ => rows = Some(values.len()),
        }
        let array = column(&name, &values)?;
        fields.push(Field::new(name, array.data_type().clone(), true));
        arrays.push(array);
    }

    let Some(rows) = rows else {
        return Err(Error::invalid("no columns"));
    };
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &options,
    )?)
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Null,
    Bool,
    Int,
    Float,
    Str,
}

fn kind(name: &str, value: Term) -> Result<Kind, Error> {
    Ok(match value.get_type() {
        TermType::Binary => Kind::Str,
        TermType::Integer => Kind::Int,
        TermType::Float => Kind::Float,
        TermType::Atom => match value
            .atom_to_string()
            .map_err(|_| Error::invalid(format!("column {name}: unsupported value")))?
            .as_str()
        {
            "nil" => Kind::Null,
            "true" | "false" => Kind::Bool,
            other => {
                return Err(Error::invalid(format!(
                    "column {name}: unsupported value :{other}"
                )))
            }
        },
        other => {
            return Err(Error::invalid(format!(
                "column {name}: unsupported value of type {other:?}"
            )))
        }
    })
}

// One kind per column; integers among floats are floats, and nil fits any.
fn column(name: &str, values: &[Term]) -> Result<ArrayRef, Error> {
    let mut column = Kind::Null;
    for &value in values {
        column = match (column, kind(name, value)?) {
            (c, Kind::Null) => c,
            (Kind::Null, k) => k,
            (c, k) if c == k => c,
            (Kind::Int, Kind::Float) | (Kind::Float, Kind::Int) => Kind::Float,
            (c, k) => {
                return Err(Error::invalid(format!(
                    "column {name} mixes {c:?} and {k:?} values"
                )))
            }
        };
    }

    let nil = |v: &Term| {
        v.get_type() == TermType::Atom && v.atom_to_string().ok().as_deref() == Some("nil")
    };
    let out_of_range = || Error::invalid(format!("column {name}: an integer is outside 64 bits"));

    Ok(match column {
        Kind::Null => new_null_array(&DataType::Null, values.len()),
        Kind::Bool => Arc::new(
            values
                .iter()
                .map(|v| {
                    if nil(v) {
                        None
                    } else {
                        v.decode::<bool>().ok()
                    }
                })
                .collect::<BooleanArray>(),
        ),
        Kind::Int => Arc::new(
            values
                .iter()
                .map(|v| {
                    if nil(v) {
                        Ok(None)
                    } else {
                        v.decode::<i64>().map(Some).map_err(|_| out_of_range())
                    }
                })
                .collect::<Result<Int64Array, _>>()?,
        ),
        Kind::Float => Arc::new(
            values
                .iter()
                .map(|v| match v.get_type() {
                    TermType::Float => v.decode::<f64>().map(Some).map_err(|_| out_of_range()),
                    TermType::Integer => v
                        .decode::<i64>()
                        .map(|i| Some(i as f64))
                        .map_err(|_| out_of_range()),
                    _ => Ok(None),
                })
                .collect::<Result<Float64Array, _>>()?,
        ),
        Kind::Str => {
            let bytes = values
                .iter()
                .map(|v| v.decode::<Binary>().map(|b| b.len()).unwrap_or(0))
                .sum();
            let mut builder = StringBuilder::with_capacity(values.len(), bytes);
            for v in values {
                if nil(v) {
                    builder.append_null();
                } else {
                    let binary = v.decode::<Binary>().map_err(|_| {
                        Error::invalid(format!("column {name}: a value is not a binary"))
                    })?;
                    let text = std::str::from_utf8(binary.as_slice()).map_err(|_| {
                        Error::invalid(format!("column {name}: a value is not UTF-8"))
                    })?;
                    builder.append_value(text);
                }
            }
            Arc::new(builder.finish())
        }
    })
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

    use arrow_array::{
        Array, Float64Array, Int32Array, Int64Array, LargeStringArray, NullArray, StringArray,
    };
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

    /// `ipc` with an 8-byte field of the first record batch's metadata,
    /// found by its value (`None`: the body length), set to `to`.
    fn patch_batch_field(mut ipc: Vec<u8>, from: Option<i64>, to: i64) -> Vec<u8> {
        let mut pos = 0;
        loop {
            let len = i32::from_le_bytes(ipc[pos + 4..pos + 8].try_into().unwrap()) as usize;
            let meta = pos + 8..pos + 8 + len;
            let body = arrow_ipc::root_as_message(&ipc[meta.clone()])
                .unwrap()
                .bodyLength();
            if body > 0 {
                let from = from.unwrap_or(body).to_le_bytes();
                let at = ipc[meta.clone()]
                    .windows(8)
                    .position(|w| w == from)
                    .expect("field");
                ipc[meta.start + at..meta.start + at + 8].copy_from_slice(&to.to_le_bytes());
                return ipc;
            }
            pos = meta.end + body as usize;
        }
    }

    fn three_ints() -> Vec<u8> {
        let input = batch(vec![("id", Arc::new(Int32Array::from(vec![1, 2, 3])))]);
        to_ipc(&input.schema(), std::slice::from_ref(&input)).unwrap()
    }

    #[test]
    fn refuses_message_lengths_beyond_the_input_before_allocating() {
        let longer = Err(Error::invalid(
            "invalid Arrow IPC stream: a message is longer than the input",
        ));

        // The batch's body claims 32 PiB.
        let bytes = patch_batch_field(three_ints(), None, 1 << 55);
        assert_eq!(from_ipc(&bytes), longer);

        let mut bytes = three_ints();
        bytes[4..8].copy_from_slice(&i32::MAX.to_le_bytes());
        assert_eq!(from_ipc(&bytes), longer);
    }

    #[test]
    fn refuses_batches_claiming_more_rows_than_their_bytes() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)]));
        let rows =
            |n| RecordBatch::try_new(schema.clone(), vec![Arc::new(NullArray::new(n))]).unwrap();

        let bytes = to_ipc(&schema, &[rows(1 << 40)]).unwrap();
        assert_eq!(
            from_ipc(&bytes),
            Err(Error::invalid(format!(
                "invalid Arrow IPC stream: {} rows cannot be in {} bytes",
                1u64 << 40,
                bytes.len()
            )))
        );
        assert!(from_ipc(&to_ipc(&schema, &[rows(1000)]).unwrap()).is_ok());
    }

    #[test]
    fn arrow_decoding_panics_are_invalid_input() {
        // The values buffer (12 bytes) claims to run far past the body.
        let bytes = patch_batch_field(three_ints(), Some(12), 1 << 40);
        let error = format!("{:?}", from_ipc(&bytes).unwrap_err());
        assert!(
            error.contains("data_invalid") && error.contains("invalid Arrow IPC stream"),
            "{error}"
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
