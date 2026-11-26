defmodule Iceberg.TableTest do
  use ExUnit.Case, async: true

  import Iceberg.TestHelpers
  alias Explorer.DataFrame, as: DF
  alias Iceberg.{Catalog, DataFiles, Error, Table}

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    catalog = catalog(dir)
    {:ok, table} = Catalog.create_table(catalog, "analytics", "events", events_schema())
    %{catalog: catalog, table: table}
  end

  test "an empty table scans to an empty batch with the table's columns", %{table: table} do
    assert {:ok, ipc} = Table.scan(table)
    df = DF.load_ipc_stream!(ipc)
    assert DF.names(df) == ["id", "name", "ts"]
    assert DF.n_rows(df) == 0
  end

  test "appended rows can be scanned back", %{table: table} do
    assert {:ok, table} = Table.append(table, events([1, 2, 3]))
    assert {:ok, table} = Table.append(table, events([4]) |> DF.dump_ipc_stream!())

    assert {:ok, ipc} = Table.scan(table)
    assert ipc |> ids() |> Enum.sort() == [1, 2, 3, 4]

    assert [%{"summary" => %{"operation" => "append"}}, _] = Table.metadata(table)["snapshots"]
  end

  test "scans take a projection, a filter and a snapshot", %{table: table} do
    {:ok, table} = Table.append(table, events([1, 2, 3]))
    first = Table.metadata(table)["current-snapshot-id"]
    {:ok, table} = Table.append(table, events([4, 5]))

    {:ok, ipc} = Table.scan(table, select: ["id"], filter: {:and, {:>=, "id", 2}, {:!=, "id", 4}})
    df = DF.load_ipc_stream!(ipc)
    assert DF.names(df) == ["id"]
    assert df |> DF.pull("id") |> Explorer.Series.to_list() |> Enum.sort() == [2, 3, 5]

    {:ok, ipc} = Table.scan(table, filter: {:in, "name", ["n1", "n5"]})
    assert ipc |> ids() |> Enum.sort() == [1, 5]

    {:ok, ipc} = Table.scan(table, filter: {:<, "ts", ~U[2026-11-19 10:00:03Z]})
    assert ipc |> ids() |> Enum.sort() == [1, 2]

    {:ok, ipc} =
      Table.scan(table, filter: {:or, {:is_null, "name"}, {:not, {:starts_with, "name", "n"}}})

    assert ids(ipc) == []

    {:ok, ipc} = Table.scan(table, snapshot_id: first)
    assert ipc |> ids() |> Enum.sort() == [1, 2, 3]
  end

  test "bad filters raise before anything runs", %{table: table} do
    assert_raise ArgumentError, ~r/unsupported filter/, fn ->
      Table.scan(table, filter: {:like, "name", "n%"})
    end

    # Filters are bound to the schema once the table has a snapshot.
    {:ok, table} = Table.append(table, events([1]))
    assert {:error, %Error{kind: :data_invalid}} = Table.scan(table, filter: {:==, "missing", 1})
  end

  test "data files are written apart from the commit, which carries snapshot properties",
       %{table: table} do
    assert {:ok, %DataFiles{files: [file]} = files} =
             Table.write_data_files(table, events([1, 2]), file_prefix: "dlv_1")

    assert %{record_count: 2, file_size_in_bytes: size} = file
    assert size > 0
    assert file.path =~ ~r{/data/dlv_1-.*\.parquet$}

    # Nothing is visible until the commit.
    {:ok, ipc} = Table.scan(table)
    assert ids(ipc) == []

    assert {:ok, table} =
             Table.fast_append(table, files, snapshot_properties: %{"app.delivery-id" => "dlv_1"})

    assert %{"summary" => %{"app.delivery-id" => "dlv_1", "added-records" => "2"}} =
             Enum.find(Table.metadata(table)["snapshots"], & &1["summary"]["app.delivery-id"])
  end

  test "commits from a stale table handle retry onto the latest metadata", %{table: table} do
    {:ok, files_a} = Table.write_data_files(table, events([1]))
    {:ok, files_b} = Table.write_data_files(table, events([2]))

    tasks =
      for files <- [files_a, files_b], do: Task.async(fn -> Table.fast_append(table, files) end)

    assert [{:ok, _}, {:ok, _}] = Task.await_many(tasks)

    {:ok, table} = Table.refresh(table)
    assert length(Table.metadata(table)["snapshots"]) == 2
    {:ok, ipc} = Table.scan(table)
    assert ipc |> ids() |> Enum.sort() == [1, 2]
  end

  test "concurrent appends to one table from one node all commit", %{catalog: catalog} do
    # Two handles on one table: commits are serialized by table, not by handle.
    {:ok, other} = Catalog.load_table(catalog, "analytics", "events")
    {:ok, table} = Catalog.load_table(catalog, "analytics", "events")

    results =
      1..24
      |> Enum.map(fn i ->
        handle = if rem(i, 2) == 0, do: table, else: other
        Task.async(fn -> Table.append(handle, events([i])) end)
      end)
      |> Task.await_many(120_000)

    assert Enum.all?(results, &match?({:ok, _}, &1)),
           inspect(Enum.reject(results, &match?({:ok, _}, &1)))

    {:ok, table} = Table.refresh(table)
    assert length(Table.metadata(table)["snapshots"]) == 24
  end

  test "metadata lists snapshots in commit order", %{table: table} do
    table = Enum.reduce(1..6, table, fn i, t -> elem(Table.append(t, events([i])), 1) end)
    metadata = Table.metadata(table)

    assert Enum.map(metadata["snapshots"], & &1["sequence-number"]) == Enum.to_list(1..6)

    assert Enum.map(metadata["snapshots"], & &1["snapshot-id"]) ==
             Enum.map(metadata["snapshot-log"], & &1["snapshot-id"])
  end

  test "input columns are cast to the table's types, missing nullable ones are null",
       %{table: table} do
    df = DF.new(%{"id" => Explorer.Series.from_list([7], dtype: :s32)})
    assert {:ok, table} = Table.append(table, df)

    {:ok, ipc} = Table.scan(table)
    df = DF.load_ipc_stream!(ipc)
    assert DF.to_rows(df) == [%{"id" => 7, "name" => nil, "ts" => nil}]
  end

  test "input that does not fit the table is refused", %{table: table} do
    assert {:error, %Error{kind: :data_invalid, message: message}} =
             Table.append(table, DF.new(%{"name" => ["x"]}))

    assert message =~ "missing required column id"

    assert {:error, %Error{kind: :data_invalid, message: message}} =
             Table.append(table, DF.new(%{"id" => [1], "extra" => [1]}))

    assert message =~ "unknown column extra"

    assert {:error, %Error{kind: :data_invalid}} = Table.append(table, "not arrow")
  end

  test "count/2 counts the rows a filter matches", %{table: table} do
    assert Table.count(table) == {:ok, 0}
    {:ok, table} = Table.append(table, events([1, 2, 3]))

    assert Table.count(table) == {:ok, 3}
    assert Table.count(table, filter: {:>, "id", 1}) == {:ok, 2}
    assert Table.count(table, filter: {:and, {:==, "name", "n3"}, {:>=, "id", 3}}) == {:ok, 1}
    assert Table.count(table, filter: {:==, "name", "none"}) == {:ok, 0}
  end

  test "columns given as lists are appended, cast to the table's types", %{table: table} do
    us = DateTime.to_unix(~U[2026-11-19 10:00:00.000001Z], :microsecond)
    columns = %{"id" => [1, 2], "name" => ["a", nil], "ts" => [us, nil]}
    assert {:ok, table} = Table.append(table, columns)

    {:ok, ipc} = Table.scan(table)

    rows =
      ipc
      |> DF.load_ipc_stream!()
      |> DF.to_rows()
      |> Enum.map(&Map.update!(&1, "ts", fn ts -> ts && DateTime.to_unix(ts, :microsecond) end))
      |> Enum.sort_by(& &1["id"])

    assert rows == [
             %{"id" => 1, "name" => "a", "ts" => us},
             %{"id" => 2, "name" => nil, "ts" => nil}
           ]
  end

  test "columns that do not fit together or with the table are refused", %{table: table} do
    for {columns, reason} <- [
          {%{"id" => [1, 2], "name" => ["a"]}, "has 1 values, another has 2"},
          {%{"id" => [1, "2"]}, "mixes"},
          {%{"id" => [1], "name" => [<<255>>]}, "not UTF-8"},
          {%{"id" => [1], "name" => [:a]}, "unsupported value :a"},
          {%{"id" => 1}, "must be a list"},
          {%{"id" => [1], "extra" => [1]}, "unknown column extra"},
          {%{}, "no columns"}
        ] do
      assert {:error, %Error{kind: :data_invalid, message: message}} =
               Table.append(table, columns)

      assert message =~ reason, inspect(columns)
    end
  end

  test "partitioned tables are written one file per partition", %{catalog: catalog} do
    {:ok, table} =
      Catalog.create_table(catalog, "analytics", "parted", events_schema(),
        partition_by: ["name"]
      )

    assert {:ok, %DataFiles{files: files} = data_files} =
             Table.write_data_files(table, events([1, 2, 3]))

    assert length(files) == 3
    assert Enum.all?(files, &(&1.path =~ ~r{/data/name=n\d/}))

    {:ok, table} = Table.fast_append(table, data_files)
    {:ok, ipc} = Table.scan(table, filter: {:==, "name", "n2"})
    assert ids(ipc) == [2]
  end

  test "to_dataframe/2 loads a scan into Explorer", %{table: table} do
    {:ok, table} = Table.append(table, events([1]))
    assert {:ok, df} = Table.to_dataframe(table, select: ["id", "name"])
    assert DF.to_rows(df) == [%{"id" => 1, "name" => "n1"}]
  end
end
