defmodule Iceberg.Native do
  @moduledoc false
  # Every function but table_metadata/1 runs on the NIF's Tokio runtime and
  # replies to the caller with `{ref, result}`; see call/2.

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :iceberg,
    crate: "iceberg_nif",
    base_url: "https://github.com/Ziinc/iceberg-ex/releases/download/v#{version}",
    version: version,
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-musl
      x86_64-pc-windows-gnu
      x86_64-pc-windows-msvc
    )

  @doc false
  def call(opts, fun) do
    ref = make_ref()

    timeout =
      case Keyword.get(opts, :timeout, :infinity) do
        :infinity -> nil
        ms when is_integer(ms) and ms > 0 -> ms
      end

    :ok = fun.(ref, timeout)

    # The NIF always replies, on its own timeout included.
    receive do
      {^ref, {:ok, :ok}} -> :ok
      {^ref, result} -> result
    end
  end

  def catalog_new(_ref, _timeout, _kind, _name, _props), do: err()
  def list_namespaces(_ref, _timeout, _catalog, _parent), do: err()
  def create_namespace(_ref, _timeout, _catalog, _ns, _props), do: err()
  def get_namespace(_ref, _timeout, _catalog, _ns), do: err()
  def namespace_exists(_ref, _timeout, _catalog, _ns), do: err()
  def drop_namespace(_ref, _timeout, _catalog, _ns), do: err()
  def list_tables(_ref, _timeout, _catalog, _ns), do: err()

  def create_table(
        _ref,
        _timeout,
        _catalog,
        _ns,
        _name,
        _schema,
        _partition,
        _props,
        _location,
        _fv
      ),
      do: err()

  def load_table(_ref, _timeout, _catalog, _ns, _name), do: err()
  def table_exists(_ref, _timeout, _catalog, _ns, _name), do: err()
  def drop_table(_ref, _timeout, _catalog, _ns, _name), do: err()
  def rename_table(_ref, _timeout, _catalog, _from_ns, _from, _to_ns, _to), do: err()

  def table_metadata(_table), do: err()
  def table_refresh(_ref, _timeout, _table), do: err()

  def table_scan(_ref, _timeout, _table, _select, _filter, _snapshot_id, _batch_size, _case),
    do: err()

  def table_write(_ref, _timeout, _table, _ipc, _prefix, _props), do: err()
  def table_fast_append(_ref, _timeout, _table, _files, _snapshot_props, _check_dup), do: err()

  defp err, do: :erlang.nif_error(:nif_not_loaded)
end
