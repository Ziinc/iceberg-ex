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

  test "to_dataframe/2 loads a scan into Explorer", %{table: table} do
    {:ok, table} = Table.append(table, events([1]))
    assert {:ok, df} = Table.to_dataframe(table, select: ["id", "name"])
    assert DF.to_rows(df) == [%{"id" => 1, "name" => "n1"}]
  end
end
