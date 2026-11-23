defmodule Iceberg.Table do
  @moduledoc """
  A loaded Iceberg table: its metadata, scans and appends.

  Table data crosses into and out of Elixir as an
  [Arrow IPC stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
  binary. With the optional `:explorer` dependency, appends also take an
  `Explorer.DataFrame` and `to_dataframe/2` loads a scan.

  A table value is a snapshot of the table's metadata at load or commit time;
  `refresh/2` reloads it. Every function but `metadata/1` takes `timeout:`
  (milliseconds, default `:infinity`).
  """
  alias Iceberg.{DataFiles, Filter, Native}

  @enforce_keys [:resource, :namespace, :name]
  defstruct [:resource, :namespace, :name]

  @type t :: %__MODULE__{resource: reference(), namespace: [String.t()], name: String.t()}
  @type data :: binary() | struct()
  @type result(ok) :: {:ok, ok} | {:error, Iceberg.Error.t()}

  @doc """
  The table's metadata in the Iceberg spec's JSON shape (string keys:
  `"format-version"`, `"current-schema-id"`, `"schemas"`, `"partition-specs"`,
  `"current-snapshot-id"`, `"snapshots"` with their `"summary"`, `"properties"`, ...).
  """
  @spec metadata(t()) :: map()
  def metadata(%__MODULE__{resource: resource}), do: Native.table_metadata(resource)

  @doc "Reloads the table from its catalog."
  @spec refresh(t(), keyword()) :: result(t())
  def refresh(%__MODULE__{} = table, opts \\ []) do
    with {:ok, resource} <- Native.call(opts, &Native.table_refresh(&1, &2, table.resource)) do
      {:ok, %{table | resource: resource}}
    end
  end

  @doc """
  Reads the table as an Arrow IPC stream.

  Options:

    * `:select` - column names (default: all)
    * `:filter` - a row filter, see `Iceberg.Filter`
    * `:snapshot_id` - read this snapshot instead of the current one
    * `:batch_size` - rows per Arrow record batch
    * `:case_sensitive` - match column names case-sensitively (default `true`)
    * `:timeout`
  """
  @spec scan(t(), keyword()) :: result(binary())
  def scan(%__MODULE__{} = table, opts \\ []) do
    filter = Filter.normalize(opts[:filter])

    Native.call(opts, fn ref, timeout ->
      Native.table_scan(
        ref,
        timeout,
        table.resource,
        opts[:select],
        filter,
        opts[:snapshot_id],
        opts[:batch_size],
        Keyword.get(opts, :case_sensitive, true)
      )
    end)
  end

  @doc """
  Writes `data` as Parquet data files in the table's location, without
  committing them. Columns are matched to the table's by name and cast to
  their types; a missing optional column is written as nulls. Partitioned
  tables get a file per partition.

  Options:

    * `:file_prefix` - the data files' name prefix (default: a random UUID).
      Files are named `<prefix>-<n>.parquet`, so a retry with the same prefix
      overwrites the files of an attempt that never committed.
    * `:properties` - `write.*` table properties to override for this write,
      such as `"write.parquet.compression-codec"` or
      `"write.target-file-size-bytes"`
    * `:timeout`
  """
  @spec write_data_files(t(), data(), keyword()) :: result(DataFiles.t())
  def write_data_files(%__MODULE__{} = table, data, opts \\ []) do
    props = Keyword.get(opts, :properties, %{})

    with {:ok, {resource, files}} <-
           Native.call(
             opts,
             &Native.table_write(&1, &2, table.resource, ipc(data), opts[:file_prefix], props)
           ) do
      {:ok, %DataFiles{resource: resource, files: files}}
    end
  end

  @doc """
  Commits data files to the table as a new append snapshot. If the table has
  moved on since `table` was loaded, the commit is retried on the latest
  metadata, as iceberg-rust's `commit.retry.*` table properties allow.

  Options:

    * `:snapshot_properties` - extra entries for the snapshot's summary
    * `:check_duplicate` - refuse files already in the table (default `true`)
    * `:timeout`
  """
  @spec fast_append(t(), DataFiles.t() | [DataFiles.t()], keyword()) :: result(t())
  def fast_append(%__MODULE__{} = table, data_files, opts \\ []) do
    resources = data_files |> List.wrap() |> Enum.map(& &1.resource)
    props = Keyword.get(opts, :snapshot_properties, %{})
    check = Keyword.get(opts, :check_duplicate, true)

    with {:ok, resource} <-
           Native.call(
             opts,
             &Native.table_fast_append(&1, &2, table.resource, resources, props, check)
           ) do
      {:ok, %{table | resource: resource}}
    end
  end

  @doc """
  Writes and commits `data` in one go: `write_data_files/3`, then
  `fast_append/3`, taking both functions' options.
  """
  @spec append(t(), data(), keyword()) :: result(t())
  def append(%__MODULE__{} = table, data, opts \\ []) do
    with {:ok, files} <- write_data_files(table, data, opts) do
      fast_append(table, files, opts)
    end
  end

  if Code.ensure_loaded?(Explorer.DataFrame) do
    @doc "Scans the table (see `scan/2`) into an `Explorer.DataFrame`."
    @spec to_dataframe(t(), keyword()) :: result(Explorer.DataFrame.t())
    def to_dataframe(%__MODULE__{} = table, opts \\ []) do
      with {:ok, ipc} <- scan(table, opts), do: Explorer.DataFrame.load_ipc_stream(ipc)
    end

    defp ipc(%Explorer.DataFrame{} = df), do: Explorer.DataFrame.dump_ipc_stream!(df)
  end

  defp ipc(data) when is_binary(data), do: data
end
