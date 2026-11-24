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
  Creates an unpartitioned table. `schema` is in the spec's JSON shape:

      %{"type" => "struct", "fields" => [
        %{"id" => 1, "name" => "id", "required" => true, "type" => "long"},
        %{"id" => 2, "name" => "name", "required" => false, "type" => "string"}
      ]}

  Options: `:properties` (table properties) and `:timeout`.
  """
  @spec create_table(t(), namespace(), String.t(), map(), keyword()) :: result(Table.t())
  def create_table(%__MODULE__{} = c, namespace, name, schema, opts \\ []) do
    props = Keyword.get(opts, :properties, %{})

    opts
    |> Native.call(&Native.create_table(&1, &2, c.resource, ns(namespace), name, schema, props))
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
end
