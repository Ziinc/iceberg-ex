defmodule Iceberg.Schema do
  @moduledoc """
  Builds an Iceberg schema (in the spec's JSON shape) from field tuples.

      Iceberg.Schema.new([
        {"id", :long, required: true},
        {"tags", {:list, :string}},
        {"point", {:struct, [{"x", :double}, {"y", :double}]}}
      ])

  Field ids are assigned in order, top-level fields first. Types: `:boolean`,
  `:int`, `:long`, `:float`, `:double`, `:date`, `:time`, `:timestamp`,
  `:timestamptz`, `:timestamp_ns`, `:timestamptz_ns`, `:string`, `:uuid`,
  `:binary`, `{:decimal, precision, scale}`, `{:fixed, length}`,
  `{:list, type}`, `{:map, key_type, value_type}` and `{:struct, fields}`.
  Field options: `:required` (default `false`) and `:doc`.
  """

  @primitives ~w(boolean int long float double date time timestamp timestamptz
                 timestamp_ns timestamptz_ns string uuid binary)a

  @type field :: {String.t(), type()} | {String.t(), type(), keyword()}
  @type type :: atom() | tuple()

  @spec new([field()]) :: map()
  def new(fields) do
    {fields, _next} = struct_fields(fields, 1)
    %{"type" => "struct", "schema-id" => 0, "fields" => fields}
  end

  # A struct's own fields take ids first, then their nested types do.
  defp struct_fields(fields, next) do
    fields = Enum.map(fields, &normalize/1)
    ids = Enum.to_list(next..(next + length(fields) - 1)//1)

    Enum.zip(fields, ids)
    |> Enum.map_reduce(next + length(fields), fn {{name, type, opts}, id}, next ->
      {type, next} = type(type, next)

      field =
        %{
          "id" => id,
          "name" => name,
          "required" => Keyword.get(opts, :required, false),
          "type" => type
        }
        |> put_doc(opts[:doc])

      {field, next}
    end)
  end

  defp normalize({name, type}), do: {name, type, []}
  defp normalize({name, type, opts}), do: {name, type, opts}

  defp put_doc(field, nil), do: field
  defp put_doc(field, doc), do: Map.put(field, "doc", doc)

  defp type(t, next) when t in @primitives, do: {Atom.to_string(t), next}
  defp type({:decimal, p, s}, next), do: {"decimal(#{p}, #{s})", next}
  defp type({:fixed, n}, next), do: {"fixed[#{n}]", next}

  defp type({:list, element}, next) do
    {element, after_element} = type(element, next + 1)

    {%{"type" => "list", "element-id" => next, "element" => element, "element-required" => false},
     after_element}
  end

  defp type({:map, key, value}, next) do
    {key, after_key} = type(key, next + 2)
    {value, after_value} = type(value, after_key)

    {%{
       "type" => "map",
       "key-id" => next,
       "key" => key,
       "value-id" => next + 1,
       "value" => value,
       "value-required" => false
     }, after_value}
  end

  defp type({:struct, fields}, next) do
    {fields, next} = struct_fields(fields, next)
    {%{"type" => "struct", "fields" => fields}, next}
  end

  defp type(other, _next), do: raise(ArgumentError, "unknown Iceberg type #{inspect(other)}")
end
