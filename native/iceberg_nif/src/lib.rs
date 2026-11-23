//! `Iceberg.Native`: iceberg-rust for Elixir.
//!
//! Catalog and table calls run on a Tokio runtime and reply to the calling
//! process with a message, so no BEAM scheduler (dirty or not) waits on I/O.

mod batches;
mod catalog;
mod error;
mod filter;
mod table;
mod term;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::LazyLock;
use std::time::Duration;

use futures::FutureExt;
use rustler::{Atom, Encoder, Env, OwnedEnv, Term};

use error::Error;

mod atoms {
    rustler::atoms! { ok, error }
}

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("iceberg-nif")
        .enable_all()
        .build()
        .expect("tokio runtime")
});

/// Runs `fut` on the runtime, then sends `{reply_ref, {:ok, value}}` or
/// `{reply_ref, {:error, %Iceberg.Error{}}}` to the calling process. The reply
/// always comes: a timeout or a panic is an error too.
pub(crate) fn spawn<T, F>(env: Env, reply_ref: Term, timeout_ms: Option<u64>, fut: F) -> Atom
where
    T: Encoder + Send + 'static,
    F: Future<Output = Result<T, Error>> + Send + 'static,
{
    let pid = env.pid();
    let mut owned = OwnedEnv::new();
    let reply_ref = owned.save(reply_ref);

    RUNTIME.spawn(async move {
        let run = AssertUnwindSafe(fut).catch_unwind();
        let result = match timeout_ms {
            Some(ms) => tokio::time::timeout(Duration::from_millis(ms), run)
                .await
                .unwrap_or_else(|_| Ok(Err(Error::timeout(ms)))),
            None => run.await,
        }
        .unwrap_or_else(|_| Err(Error::new("unexpected", "the NIF panicked")));

        // Fails only if the caller has exited, and then no one is waiting.
        let _ = owned.send_and_clear(&pid, |env| {
            let reply = match result {
                Ok(value) => (atoms::ok(), value).encode(env),
                Err(e) => (atoms::error(), e).encode(env),
            };
            (reply_ref.load(env), reply)
        });
    });

    atoms::ok()
}

rustler::init!("Elixir.Iceberg.Native");
