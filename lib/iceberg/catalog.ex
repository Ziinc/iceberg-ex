defmodule Iceberg.Catalog do
  @moduledoc """
  An Iceberg catalog: an Iceberg REST catalog, Amazon S3 Tables, or an
  in-memory catalog (for tests and local work).

      {:ok, catalog} =
        Iceberg.Catalog.new(:rest, %{
          "uri" => "https://catalog.example.com",
          "warehouse" => "analytics",
          "token" => token,
          "s3.region" => "eu-west-1"
        })

  Properties are iceberg-rust's, as string keys and values: the catalog's own
  (`uri`, `warehouse`, `token`, `credential`, `table_bucket_arn`, ...) and the
  storage's (`s3.endpoint`, `s3.access-key-id`, `s3.path-style-access`, ...).
  The memory catalog takes `warehouse`, such as `"file:///tmp/warehouse"`.

  A namespace is a string (one level) or a list of levels. Every function
  takes `timeout:` (milliseconds, default `:infinity`).
  """
  alias Iceberg.{Native, Table}

  @enforce_keys [:resource, :kind]
  defstruct [:resource, :kind]

  @type t :: %__MODULE__{resource: reference(), kind: atom()}
  @type namespace :: String.t() | [String.t()]
  @type result(ok) :: {:ok, ok} | {:error, Iceberg.Error.t()}

  @doc """
  Connects to a catalog of `kind` (`:rest`, `:s3tables` or `:memory`).

  Options: `:name` (the catalog's name, default `"iceberg"`) and `:timeout`.
  """
  @spec new(atom(), %{String.t() => String.t()}, keyword()) :: result(t())
  def new(kind, props, opts \\ []) do
    name = Keyword.get(opts, :name, "iceberg")

    with {:ok, resource} <-
           Native.call(opts, &Native.catalog_new(&1, &2, Atom.to_string(kind), name, props)) do
      {:ok, %__MODULE__{resource: resource, kind: kind}}
    end
  end

  @doc "The namespaces under `parent` (top-level ones by default), as lists of levels."
  @spec list_namespaces(t(), namespace() | nil, keyword()) :: result([[String.t()]])
  def list_namespaces(%__MODULE__{} = c, parent \\ nil, opts \\ []) do
    parent = if parent, do: ns(parent)
    Native.call(opts, &Native.list_namespaces(&1, &2, c.resource, parent))
  end

  @spec create_namespace(t(), namespace(), map(), keyword()) :: :ok | {:error, Iceberg.Error.t()}
  def create_namespace(%__MODULE__{} = c, namespace, props \\ %{}, opts \\ []) do
    Native.call(opts, &Native.create_namespace(&1, &2, c.resource, ns(namespace), props))
  end

  @doc "The namespace's properties."
  @spec get_namespace(t(), namespace(), keyword()) :: result(%{String.t() => String.t()})
  def get_namespace(%__MODULE__{} = c, namespace, opts \\ []) do
    Native.call(opts, &Native.get_namespace(&1, &2, c.resource, ns(namespace)))
  end

  @spec namespace_exists(t(), namespace(), keyword()) :: result(boolean())
  def namespace_exists(%__MODULE__{} = c, namespace, opts \\ []) do
    Native.call(opts, &Native.namespace_exists(&1, &2, c.resource, ns(namespace)))
  end

  @spec drop_namespace(t(), namespace(), keyword()) :: :ok | {:error, Iceberg.Error.t()}
  def drop_namespace(%__MODULE__{} = c, namespace, opts \\ []) do
    Native.call(opts, &Native.drop_namespace(&1, &2, c.resource, ns(namespace)))
  end

  @doc "The names of the tables in a namespace."
  @spec list_tables(t(), namespace(), keyword()) :: result([String.t()])
  def list_tables(%__MODULE__{} = c, namespace, opts \\ []) do
    Native.call(opts, &Native.list_tables(&1, &2, c.resource, ns(namespace)))
  end

  @doc """
  Creates a table. `schema` is in the spec's JSON shape, as built by
  `Iceberg.Schema.new/1`.

  Options:

    * `:partition_by` - partition fields, each a source column name
      (identity) or `{column, transform}` / `{column, transform, name}`, where
      transform is `:identity`, `:year`, `:month`, `:day`, `:hour`, `:void`,
      `{:bucket, n}` or `{:truncate, width}`
    * `:properties` - table properties
    * `:location` - the table's location (default: chosen by the catalog)
    * `:format_version` - 1, 2 (default) or 3
    * `:timeout`
  """
  @spec create_table(t(), namespace(), String.t(), map(), keyword()) :: result(Table.t())
  def create_table(%__MODULE__{} = c, namespace, name, schema, opts \\ []) do
    partition = opts |> Keyword.get(:partition_by, []) |> Enum.map(&partition_field/1)
    props = Keyword.get(opts, :properties, %{})

    Native.call(opts, fn ref, timeout ->
      Native.create_table(
        ref,
        timeout,
        c.resource,
        ns(namespace),
        name,
        schema,
        partition,
        props,
        opts[:location],
        opts[:format_version]
      )
    end)
    |> table(namespace, name)
  end

  @spec load_table(t(), namespace(), String.t(), keyword()) :: result(Table.t())
  def load_table(%__MODULE__{} = c, namespace, name, opts \\ []) do
    opts
    |> Native.call(&Native.load_table(&1, &2, c.resource, ns(namespace), name))
    |> table(namespace, name)
  end

  @spec table_exists(t(), namespace(), String.t(), keyword()) :: result(boolean())
  def table_exists(%__MODULE__{} = c, namespace, name, opts \\ []) do
    Native.call(opts, &Native.table_exists(&1, &2, c.resource, ns(namespace), name))
  end

  @doc "Drops a table from the catalog, leaving its files."
  @spec drop_table(t(), namespace(), String.t(), keyword()) :: :ok | {:error, Iceberg.Error.t()}
  def drop_table(%__MODULE__{} = c, namespace, name, opts \\ []) do
    Native.call(opts, &Native.drop_table(&1, &2, c.resource, ns(namespace), name))
  end

  @spec rename_table(t(), {namespace(), String.t()}, {namespace(), String.t()}, keyword()) ::
          :ok | {:error, Iceberg.Error.t()}
  def rename_table(%__MODULE__{} = c, {from_ns, from}, {to_ns, to}, opts \\ []) do
    Native.call(opts, &Native.rename_table(&1, &2, c.resource, ns(from_ns), from, ns(to_ns), to))
  end

  defp table({:ok, resource}, namespace, name),
    do: {:ok, %Table{resource: resource, namespace: ns(namespace), name: name}}

  defp table(error, _namespace, _name), do: error

  defp ns(namespace) when is_binary(namespace), do: [namespace]
  defp ns(namespace) when is_list(namespace), do: namespace

  # {source column, partition field name, transform}, as Iceberg's Java API names them.
  defp partition_field({column, transform, name}), do: {column, name, transform(transform)}

  defp partition_field({column, transform}) do
    spec = transform(transform)
    {column, field_name(column, transform), spec}
  end

  defp partition_field(column) when is_binary(column), do: partition_field({column, :identity})

  defp field_name(column, :identity), do: column
  defp field_name(column, :void), do: "#{column}_null"
  defp field_name(column, {:bucket, _}), do: "#{column}_bucket"
  defp field_name(column, {:truncate, _}), do: "#{column}_trunc"
  defp field_name(column, t) when t in [:year, :month, :day, :hour], do: "#{column}_#{t}"

  defp transform({:bucket, n}) when is_integer(n) and n > 0, do: "bucket[#{n}]"
  defp transform({:truncate, w}) when is_integer(w) and w > 0, do: "truncate[#{w}]"

  defp transform(t) when t in [:identity, :year, :month, :day, :hour, :void],
    do: Atom.to_string(t)

  defp transform(t), do: raise(ArgumentError, "unknown partition transform #{inspect(t)}")
end
