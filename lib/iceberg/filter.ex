defmodule Iceberg.Filter do
  @moduledoc """
  Row filters for `Iceberg.Table.scan/2`, as tuples:

    * `{op, column, value}` with op `:==`, `:!=`, `:<`, `:<=`, `:>`, `:>=`,
      `:starts_with` or `:not_starts_with`
    * `{:in, column, values}` and `{:not_in, column, values}`
    * `{:is_null, column}`, `{:not_null, column}`, `{:is_nan, column}` and
      `{:not_nan, column}`
    * `{:and, a, b}`, `{:or, a, b}`, `{:not, a}`, `true` and `false`

  Values are 64-bit integers, floats, UTF-8 strings, booleans, `Date`,
  `Time`, `NaiveDateTime` (a `timestamp` column) and `DateTime`
  (`timestamptz`). Iceberg converts a value to its column's type when it
  binds the filter. Filters nest at most 128 levels deep.
  """

  @compare [:==, :!=, :<, :<=, :>, :>=, :starts_with, :not_starts_with]
  @unary [:is_null, :not_null, :is_nan, :not_nan]

  # The NIF decodes filters recursively on a scheduler thread.
  @max_depth 128

  @doc false
  # The filter with each value as the NIF reads it; raises on anything else.
  def normalize(nil), do: nil
  def normalize(filter), do: normalize(filter, 0)

  defp normalize(_filter, depth) when depth > @max_depth,
    do: raise(ArgumentError, "filter nested more than #{@max_depth} levels")

  defp normalize(bool, _depth) when is_boolean(bool), do: bool

  defp normalize({op, a, b}, depth) when op in [:and, :or],
    do: {op, normalize(a, depth + 1), normalize(b, depth + 1)}

  defp normalize({:not, a}, depth), do: {:not, normalize(a, depth + 1)}

  defp normalize({op, col, value}, _depth) when op in @compare and is_binary(col),
    do: {op, col, value(value)}

  defp normalize({op, col, values}, _depth)
       when op in [:in, :not_in] and is_binary(col) and is_list(values),
       do: {op, col, Enum.map(values, &value/1)}

  defp normalize({op, col}, _depth) when op in @unary and is_binary(col), do: {op, col}

  defp normalize(other, _depth),
    do: raise(ArgumentError, "unsupported filter #{inspect(other, limit: 5)}")

  @epoch ~N[1970-01-01 00:00:00]

  defp value(v) when is_integer(v) and v in -0x8000000000000000..0x7FFFFFFFFFFFFFFF, do: v

  defp value(v) when is_integer(v),
    do: raise(ArgumentError, "filter integer #{v} is outside the 64-bit range")

  defp value(v) when is_binary(v) do
    if String.valid?(v), do: v, else: raise(ArgumentError, "filter strings must be valid UTF-8")
  end

  defp value(v) when is_float(v) or is_boolean(v), do: v
  defp value(%Date{} = d), do: {:date, Date.diff(d, ~D[1970-01-01])}
  defp value(%Time{} = t), do: {:time, Time.diff(t, ~T[00:00:00], :microsecond)}
  defp value(%NaiveDateTime{} = t), do: {:timestamp, NaiveDateTime.diff(t, @epoch, :microsecond)}
  defp value(%DateTime{} = t), do: {:timestamptz, DateTime.to_unix(t, :microsecond)}
  defp value(other), do: raise(ArgumentError, "unsupported filter value #{inspect(other)}")
end
