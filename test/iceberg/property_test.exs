defmodule Iceberg.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Iceberg.TestHelpers, only: [catalog: 1, events_schema: 0, events: 1]
  alias Explorer.DataFrame, as: DF
  alias Iceberg.{Catalog, Error, Schema, Table}

  @moduletag :tmp_dir
  @i64 -0x8000000000000000..0x7FFFFFFFFFFFFFFF

  setup %{tmp_dir: dir}, do: %{catalog: catalog(dir)}

  defp table(catalog, schema \\ events_schema(), opts \\ []) do
    name = "t#{System.unique_integer([:positive])}"
    {:ok, table} = Catalog.create_table(catalog, "analytics", name, schema, opts)
    table
  end

  # Rows appended in `chunks` (some possibly empty), so a scan reads several files.
  defp append_chunks(table, chunks) do
    Enum.reduce(chunks, table, fn rows, table ->
      df =
        DF.new(%{
          "id" => Explorer.Series.from_list(Enum.map(rows, & &1["id"]), dtype: :s64),
          "name" => Explorer.Series.from_list(Enum.map(rows, & &1["name"]), dtype: :string),
          "ts" =>
            Explorer.Series.from_list(Enum.map(rows, & &1["ts"]),
              dtype: {:datetime, :microsecond, "Etc/UTC"}
            )
        })

      {:ok, table} = Table.append(table, df)
      table
    end)
  end

  defp scanned(table, opts \\ []) do
    {:ok, ipc} = Table.scan(table, opts)
    ipc |> DF.load_ipc_stream!() |> DF.to_rows() |> comparable()
  end

  # Explorer returns timestamptz values in "UTC" rather than "Etc/UTC": compare instants.
  defp comparable(rows) do
    rows
    |> Enum.map(&Map.update!(&1, "ts", fn ts -> ts && DateTime.to_unix(ts, :microsecond) end))
    |> Enum.sort()
  end

  defp row(name_gen) do
    gen all id <- one_of([integer(), integer(@i64)]),
            name <- name_gen,
            us <- one_of([constant(nil), integer(-2_208_988_800_000_000..7_258_118_400_000_000)]) do
      %{"id" => id, "name" => name, "ts" => us && DateTime.from_unix!(us, :microsecond)}
    end
  end

  defp chunks(row_gen),
    do: list_of(list_of(row_gen, max_length: 25), min_length: 1, max_length: 4)

  property "appended rows scan back unchanged", %{catalog: catalog} do
    check all chunks <- chunks(row(one_of([constant(nil), string(:utf8, max_length: 12)]))),
              max_runs: 40 do
      table = catalog |> table() |> append_chunks(chunks)
      assert scanned(table) == chunks |> List.flatten() |> comparable()
    end
  end

  # Filters over few distinct values, so they match some rows and prune some files.
  defp filter do
    id = integer(-4..4)
    name = string(?a..?c, max_length: 2)

    leaf =
      one_of([
        boolean(),
        tuple({member_of([:==, :!=, :<, :<=, :>, :>=]), constant("id"), id}),
        tuple(
          {member_of([:==, :!=, :<, :<=, :>, :>=, :starts_with, :not_starts_with]),
           constant("name"), name}
        ),
        tuple({member_of([:in, :not_in]), constant("id"), list_of(id, max_length: 3)}),
        tuple({member_of([:in, :not_in]), constant("name"), list_of(name, max_length: 3)}),
        tuple({member_of([:is_null, :not_null]), member_of(["id", "name"])})
      ])

    tree(leaf, fn child ->
      one_of([tuple({member_of([:and, :or]), child, child}), tuple({constant(:not), child})])
    end)
  end

  defp matches?(true, _), do: true
  defp matches?(false, _), do: false
  defp matches?({:and, a, b}, row), do: matches?(a, row) and matches?(b, row)
  defp matches?({:or, a, b}, row), do: matches?(a, row) or matches?(b, row)
  defp matches?({:not, a}, row), do: not matches?(a, row)
  defp matches?({:is_null, col}, row), do: row[col] == nil
  defp matches?({:not_null, col}, row), do: row[col] != nil
  defp matches?({:in, col, vs}, row), do: row[col] in vs
  defp matches?({:not_in, col, vs}, row), do: row[col] not in vs
  defp matches?({:starts_with, col, v}, row), do: String.starts_with?(row[col], v)
  defp matches?({:not_starts_with, col, v}, row), do: not String.starts_with?(row[col], v)
  defp matches?({op, col, v}, row), do: apply(Kernel, op, [row[col], v])

  property "filtered scans return exactly the matching rows", %{catalog: catalog} do
    rows =
      gen(
        all(
          id <- integer(-4..4),
          name <- string(?a..?c, max_length: 2),
          do: %{"id" => id, "name" => name, "ts" => nil}
        )
      )

    check all chunks <- chunks(rows), filter <- filter(), max_runs: 75 do
      table = catalog |> table() |> append_chunks(chunks)
      expected = chunks |> List.flatten() |> Enum.filter(&matches?(filter, &1)) |> comparable()
      assert scanned(table, filter: filter) == expected, "filter: #{inspect(filter)}"
    end
  end

  property "any filter term returns a result or raises ArgumentError", %{catalog: catalog} do
    table = catalog |> table() |> append_chunks([[%{"id" => 1, "name" => "a", "ts" => nil}]])
    near_miss = tuple({member_of([:==, :in, :and, :not, :is_null, :like]), term(), term()})

    check all filter <- one_of([term(), near_miss, filter()]), max_runs: 300 do
      try do
        result = Table.scan(table, filter: filter)
        assert match?({:ok, _}, result) or match?({:error, %Error{}}, result)
      rescue
        ArgumentError -> :ok
      end
    end
  end

  property "corrupt Arrow input is invalid data, never a crash", %{catalog: catalog} do
    table = table(catalog)
    valid = events([1, 2, 3]) |> DF.dump_ipc_stream!()

    flipped =
      gen all flips <-
                list_of({integer(0..(byte_size(valid) - 1)), integer(0..255)},
                  min_length: 1,
                  max_length: 8
                ) do
        Enum.reduce(flips, valid, fn {at, byte}, bin ->
          <<head::binary-size(^at), _, rest::binary>> = bin
          <<head::binary, byte, rest::binary>>
        end)
      end

    check all data <- one_of([binary(), flipped]), max_runs: 300 do
      result = Table.append(table, data)

      assert match?({:ok, %Table{}}, result) or
               match?({:error, %Error{kind: :data_invalid}}, result),
             inspect(result)
    end
  end

  property "table and snapshot properties round-trip", %{catalog: catalog} do
    props =
      map_of(map(string(:printable, min_length: 1, max_length: 12), &("p." <> &1)), string(:utf8),
        max_length: 5
      )

    check all table_props <- props, snapshot_props <- props, max_runs: 30 do
      table = table(catalog, events_schema(), properties: table_props)
      stored = Map.get(Table.metadata(table), "properties", %{})
      assert Map.take(stored, Map.keys(table_props)) == table_props

      {:ok, table} = Table.append(table, events([1]), snapshot_properties: snapshot_props)
      [%{"summary" => summary}] = Table.metadata(table)["snapshots"]
      assert Map.take(summary, Map.keys(snapshot_props)) == snapshot_props
    end
  end

  defp schema_type do
    primitive =
      one_of([
        member_of(
          ~w(boolean int long float double date time timestamp timestamptz string uuid binary)a
        ),
        map(integer(1..16), &{:fixed, &1}),
        bind(integer(1..38), fn p -> map(integer(0..p), &{:decimal, p, &1}) end)
      ])

    tree(primitive, fn child ->
      one_of([
        map(child, &{:list, &1}),
        map({primitive, child}, fn {k, v} -> {:map, k, v} end),
        map(fields(child), &{:struct, &1})
      ])
    end)
  end

  defp fields(type) do
    gen all names <-
              uniq_list_of(string(:alphanumeric, min_length: 1, max_length: 6),
                min_length: 1,
                max_length: 4
              ),
            types <- list_of(type, length: length(names)),
            required <- list_of(boolean(), length: length(names)) do
      Enum.zip_with([names, types, required], fn [n, t, r] -> {n, t, required: r} end)
    end
  end

  defp field_ids(%{"fields" => fs}), do: Enum.flat_map(fs, &[&1["id"] | field_ids(&1["type"])])
  defp field_ids(%{"type" => "list"} = t), do: [t["element-id"] | field_ids(t["element"])]

  defp field_ids(%{"type" => "map"} = t),
    do: [t["key-id"], t["value-id"] | field_ids(t["key"]) ++ field_ids(t["value"])]

  defp field_ids(_primitive), do: []

  property "built schemas have distinct, contiguous ids and are stored as built", %{
    catalog: catalog
  } do
    check all fields <- fields(schema_type()), max_runs: 50 do
      schema = Schema.new(fields)
      ids = field_ids(schema)
      assert Enum.sort(ids) == Enum.to_list(1..length(ids))

      stored = catalog |> table(schema) |> Table.metadata() |> Map.fetch!("schemas") |> hd()
      assert stored["fields"] == schema["fields"]
    end
  end
end
