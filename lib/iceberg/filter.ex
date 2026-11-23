defmodule Iceberg.Filter do
  @moduledoc """
  Row filters for `Iceberg.Table.scan/2`, as tuples:

    * `{op, column, value}` with op `:==`, `:!=`, `:<`, `:<=`, `:>`, `:>=`,
      `:starts_with` or `:not_starts_with`
    * `{:in, column, values}` and `{:not_in, column, values}`
    * `{:is_null, column}`, `{:not_null, column}`, `{:is_nan, column}` and
      `{:not_nan, column}`
    * `{:and, a, b}`, `{:or, a, b}`, `{:not, a}`, `true` and `false`

  Values are integers, floats, strings, booleans, `Date`, `Time`,
  `NaiveDateTime` (a `timestamp` column) and `DateTime` (`timestamptz`).
  Iceberg converts a value to its column's type when it binds the filter.
  """

  @compare [:==, :!=, :<, :<=, :>, :>=, :starts_with, :not_starts_with]
  @unary [:is_null, :not_null, :is_nan, :not_nan]

  @doc false
  # The filter with each value as the NIF reads it; raises on anything else.
  def normalize(nil), do: nil
  def normalize(bool) when is_boolean(bool), do: bool
  def normalize({op, a, b}) when op in [:and, :or], do: {op, normalize(a), normalize(b)}
  def normalize({:not, a}), do: {:not, normalize(a)}

  def normalize({op, col, value}) when op in @compare and is_binary(col),
    do: {op, col, value(value)}

  def normalize({op, col, values})
      when op in [:in, :not_in] and is_binary(col) and is_list(values),
      do: {op, col, Enum.map(values, &value/1)}

  def normalize({op, col}) when op in @unary and is_binary(col), do: {op, col}
  def normalize(other), do: raise(ArgumentError, "unsupported filter #{inspect(other)}")

  @epoch ~N[1970-01-01 00:00:00]

  defp value(v) when is_integer(v) or is_float(v) or is_binary(v) or is_boolean(v), do: v
  defp value(%Date{} = d), do: {:date, Date.diff(d, ~D[1970-01-01])}
  defp value(%Time{} = t), do: {:time, Time.diff(t, ~T[00:00:00], :microsecond)}
  defp value(%NaiveDateTime{} = t), do: {:timestamp, NaiveDateTime.diff(t, @epoch, :microsecond)}
  defp value(%DateTime{} = t), do: {:timestamptz, DateTime.to_unix(t, :microsecond)}
  defp value(other), do: raise(ArgumentError, "unsupported filter value #{inspect(other)}")
end
