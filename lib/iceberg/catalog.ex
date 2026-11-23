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

  @spec create_namespace(t(), namespace(), map(), keyword()) :: :ok | {:error, Iceberg.Error.t()}
  def create_namespace(%__MODULE__{} = c, namespace, props \\ %{}, opts \\ []) do
    Native.call(opts, &Native.create_namespace(&1, &2, c.resource, ns(namespace), props))
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

  defp table({:ok, resource}, namespace, name),
    do: {:ok, %Table{resource: resource, namespace: ns(namespace), name: name}}

  defp table(error, _namespace, _name), do: error

  defp ns(namespace) when is_binary(namespace), do: [namespace]
  defp ns(namespace) when is_list(namespace), do: namespace
end
