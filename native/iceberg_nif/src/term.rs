//! Elixir terms to and from JSON values and binaries.

use rustler::env::SavedTerm;
use rustler::types::map::MapIterator;
use rustler::{Binary, Encoder, Env, NewBinary, NifResult, OwnedEnv, Term, TermType};
use serde_json::{Map, Number, Value};

use crate::error::arg_error;

/// How deeply `to_json/1` and filters may nest: recursion runs on a
/// scheduler thread, whose stack a much deeper term would overflow.
pub const MAX_DEPTH: usize = 128;

/// Elixir data (maps, lists, strings, numbers, booleans, `nil`, atoms) as JSON.
pub fn to_json(term: Term) -> NifResult<Value> {
    json(term, 0)
}

fn json(term: Term, depth: usize) -> NifResult<Value> {
    if depth > MAX_DEPTH {
        return Err(arg_error(format!(
            "data nested more than {MAX_DEPTH} levels"
        )));
    }
    let to_json = |t| json(t, depth + 1);
    Ok(match term.get_type() {
        TermType::Atom => match term.atom_to_string()?.as_str() {
            "nil" => Value::Null,
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            other => Value::String(other.into()),
        },
        TermType::Binary => Value::String(term.decode()?),
        TermType::Integer => Value::from(term.decode::<i64>()?),
        TermType::Float => Number::from_f64(term.decode()?)
            .map(Value::Number)
            .ok_or(rustler::Error::BadArg)?,
        TermType::List => Value::Array(
            term.decode::<Vec<Term>>()?
                .into_iter()
                .map(to_json)
                .collect::<NifResult<_>>()?,
        ),
        TermType::Map => {
            let mut map = Map::new();
            for (k, v) in MapIterator::new(term).ok_or(rustler::Error::BadArg)? {
                let Value::String(key) = to_json(k)? else {
                    return Err(rustler::Error::BadArg);
                };
                map.insert(key, to_json(v)?);
            }
            Value::Object(map)
        }
        _ => return Err(rustler::Error::BadArg),
    })
}

/// JSON as Elixir data; objects become maps with string keys.
pub struct Json(pub Value);

impl Encoder for Json {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        encode_json(env, &self.0)
    }
}

fn encode_json<'a>(env: Env<'a>, value: &Value) -> Term<'a> {
    match value {
        Value::Null => rustler::types::atom::nil().encode(env),
        Value::Bool(b) => b.encode(env),
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i.encode(env),
            (_, Some(u)) => u.encode(env),
            _ => n.as_f64().unwrap_or(f64::NAN).encode(env),
        },
        Value::String(s) => s.encode(env),
        Value::Array(items) => items
            .iter()
            .map(|v| encode_json(env, v))
            .collect::<Vec<_>>()
            .encode(env),
        Value::Object(map) => {
            let keys: Vec<Term> = map.keys().map(|k| k.encode(env)).collect();
            let values: Vec<Term> = map.values().map(|v| encode_json(env, v)).collect();
            Term::map_from_arrays(env, &keys, &values).expect("unique keys")
        }
    }
}

/// Bytes as an Elixir binary.
pub struct Bin(pub Vec<u8>);

impl Encoder for Bin {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let mut bin = NewBinary::new(env, self.0.len());
        bin.as_mut_slice().copy_from_slice(&self.0);
        Binary::from(bin).to_term(env)
    }
}

/// An Elixir binary kept alive (not copied) for reading off the scheduler.
pub struct SavedBinary {
    env: OwnedEnv,
    term: SavedTerm,
}

impl SavedBinary {
    pub fn new(binary: Binary) -> Self {
        let env = OwnedEnv::new();
        let term = env.save(binary);
        SavedBinary { env, term }
    }

    pub fn with<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        self.env.run(|env| {
            f(self
                .term
                .load(env)
                .decode::<Binary>()
                .expect("a saved binary")
                .as_slice())
        })
    }
}
