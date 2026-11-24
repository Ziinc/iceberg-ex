//! Row filters (`Iceberg.Filter`, normalized) as iceberg-rust predicates.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use rustler::types::tuple::get_tuple;
use rustler::{Error::BadArg, NifResult, Term, TermType};

use crate::error::arg_error;
use crate::term::MAX_DEPTH;

pub fn decode(term: Term) -> NifResult<Option<Predicate>> {
    if term.get_type() == TermType::Atom && term.atom_to_string()? == "nil" {
        return Ok(None);
    }
    predicate(term, 0).map(Some)
}

fn predicate(term: Term, depth: usize) -> NifResult<Predicate> {
    if depth > MAX_DEPTH {
        return Err(arg_error(format!(
            "filter nested more than {MAX_DEPTH} levels"
        )));
    }
    if let Ok(b) = term.decode::<bool>() {
        return Ok(if b {
            Predicate::AlwaysTrue
        } else {
            Predicate::AlwaysFalse
        });
    }
    let items = get_tuple(term)?;
    let op = items.first().ok_or(BadArg)?.atom_to_string()?;
    let col = || -> NifResult<Reference> { Ok(Reference::new(items[1].decode::<String>()?)) };
    let list = || -> NifResult<Vec<Datum>> {
        items[2]
            .decode::<Vec<Term>>()?
            .into_iter()
            .map(datum)
            .collect()
    };

    Ok(match (op.as_str(), items.len()) {
        ("and", 3) => predicate(items[1], depth + 1)?.and(predicate(items[2], depth + 1)?),
        ("or", 3) => predicate(items[1], depth + 1)?.or(predicate(items[2], depth + 1)?),
        ("not", 2) => !predicate(items[1], depth + 1)?,
        ("==", 3) => col()?.equal_to(datum(items[2])?),
        ("!=", 3) => col()?.not_equal_to(datum(items[2])?),
        ("<", 3) => col()?.less_than(datum(items[2])?),
        ("<=", 3) => col()?.less_than_or_equal_to(datum(items[2])?),
        (">", 3) => col()?.greater_than(datum(items[2])?),
        (">=", 3) => col()?.greater_than_or_equal_to(datum(items[2])?),
        ("starts_with", 3) => col()?.starts_with(datum(items[2])?),
        ("not_starts_with", 3) => col()?.not_starts_with(datum(items[2])?),
        ("in", 3) => col()?.is_in(list()?),
        ("not_in", 3) => col()?.is_not_in(list()?),
        ("is_null", 2) => col()?.is_null(),
        ("not_null", 2) => col()?.is_not_null(),
        ("is_nan", 2) => col()?.is_nan(),
        ("not_nan", 2) => col()?.is_not_nan(),
        _ => return Err(BadArg),
    })
}

/// Integers are longs and floats doubles; binding converts them to the column's type.
fn datum(term: Term) -> NifResult<Datum> {
    match term.get_type() {
        TermType::Integer => Ok(Datum::long(term.decode::<i64>()?)),
        TermType::Float => Ok(Datum::double(term.decode::<f64>()?)),
        TermType::Binary => Ok(Datum::string(term.decode::<String>()?)),
        TermType::Atom => Ok(Datum::bool(term.decode::<bool>()?)),
        TermType::Tuple => {
            let items = get_tuple(term)?;
            let [tag, value] = items[..] else {
                return Err(BadArg);
            };
            let value: i64 = value.decode()?;
            match tag.atom_to_string()?.as_str() {
                "date" => Ok(Datum::date(i32::try_from(value).map_err(|_| BadArg)?)),
                "time" => Datum::time_micros(value).map_err(|_| BadArg),
                "timestamp" => Ok(Datum::timestamp_micros(value)),
                "timestamptz" => Ok(Datum::timestamptz_micros(value)),
                _ => Err(BadArg),
            }
        }
        _ => Err(BadArg),
    }
}
