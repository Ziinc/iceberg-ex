ExUnit.start()

defmodule Iceberg.TestHelpers do
  @moduledoc false
  alias Explorer.DataFrame, as: DF

  @doc "An in-memory catalog whose files live under `dir`, with namespace `ns`."
  def catalog(dir, ns \\ "analytics") do
    {:ok, catalog} = Iceberg.Catalog.new(:memory, %{"warehouse" => "file://#{dir}"})
    :ok = Iceberg.Catalog.create_namespace(catalog, ns)
    catalog
  end

  def events_schema do
    Iceberg.Schema.new([
      {"id", :long, required: true},
      {"name", :string},
      {"ts", :timestamptz}
    ])
  end

  def events(ids) do
    DF.new(%{
      "id" => ids,
      "name" => Enum.map(ids, &"n#{&1}"),
      "ts" => Enum.map(ids, fn i -> DateTime.add(~U[2026-11-19 10:00:00.000000Z], i, :second) end)
    })
  end

  def ids(ipc), do: ipc |> DF.load_ipc_stream!() |> DF.pull("id") |> Explorer.Series.to_list()
end
