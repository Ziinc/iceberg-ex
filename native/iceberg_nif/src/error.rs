//! `%Iceberg.Error{kind, message, retryable}`.

use rustler::{Atom, Encoder, Env, Term};

#[derive(Debug, PartialEq)]
pub struct Error {
    kind: String,
    message: String,
    retryable: bool,
}

impl Error {
    pub fn new(kind: &str, message: impl Into<String>) -> Self {
        Error {
            kind: kind.into(),
            message: message.into(),
            retryable: false,
        }
    }

    /// Input that does not fit: bad Arrow data, schema or arguments.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("data_invalid", message)
    }

    pub fn timeout(ms: u64) -> Self {
        Error {
            kind: "timeout".into(),
            message: format!("timed out after {ms} ms"),
            retryable: true,
        }
    }
}

impl From<iceberg::Error> for Error {
    fn from(e: iceberg::Error) -> Self {
        Error {
            kind: snake_case(&format!("{:?}", e.kind())),
            message: e.to_string(),
            retryable: e.retryable(),
        }
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::invalid(e.to_string())
    }
}

fn snake_case(camel: &str) -> String {
    let mut out = String::with_capacity(camel.len() + 4);
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

impl Encoder for Error {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let atom = |s: &str| Atom::from_str(env, s).expect("atom").encode(env);
        let keys = [
            atom("__struct__"),
            atom("__exception__"),
            atom("kind"),
            atom("message"),
            atom("retryable"),
        ];
        let values = [
            atom("Elixir.Iceberg.Error"),
            true.encode(env),
            atom(&self.kind),
            self.message.encode(env),
            self.retryable.encode(env),
        ];
        Term::map_from_arrays(env, &keys, &values).expect("unique keys")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_snake_case_atoms() {
        assert_eq!(snake_case("TableNotFound"), "table_not_found");
        assert_eq!(snake_case("Unexpected"), "unexpected");
        let e: Error = iceberg::Error::new(iceberg::ErrorKind::CatalogCommitConflicts, "x").into();
        assert_eq!(e.kind, "catalog_commit_conflicts");
    }
}
