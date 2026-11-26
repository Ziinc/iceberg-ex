defmodule Iceberg.EdgeCasesTest do
  # Regressions from chaos testing: input that must be refused, or
  # accepted without changing the data, and never crash the VM.
  use ExUnit.Case, async: true

  import Iceberg.TestHelpers
  alias Explorer.DataFrame, as: DF
  alias Iceberg.{Catalog, DataFiles, Error, Native, Table}

  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    catalog = catalog(dir)
    {:ok, table} = Catalog.create_table(catalog, "analytics", "events", events_schema())
    %{catalog: catalog, table: table}
  end

  defp nest(levels, inner, wrap), do: Enum.reduce(1..levels, inner, fn _, acc -> wrap.(acc) end)

  describe "appends" do
    test "no rows leave the table as it was", %{table: table} do
      {:ok, empty} = Table.scan(table)
      assert {:ok, %DataFiles{files: []} = files} = Table.write_data_files(table, empty)
      assert {:ok, table} = Table.fast_append(table, files)
      assert {:ok, table} = Table.fast_append(table, [])
      assert {:ok, table} = Table.append(table, empty)
      assert Map.get(Table.metadata(table), "snapshots", []) == []
    end

    test "data must be columns, an Arrow IPC stream or a DataFrame", %{table: table} do
      assert_raise ArgumentError, ~r/expected an Arrow IPC stream binary, a map of columns/, fn ->
        Table.append(table, [%{"id" => 1}])
      end
    end

    test "duplicate input columns are refused, not dropped", %{table: table} do
      ipc = DF.new(%{"id" => [1], "zq" => [2]}) |> DF.dump_ipc_stream!()
      dup = :binary.replace(ipc, <<2, 0, 0, 0, "zq">>, <<2, 0, 0, 0, "id">>)

      assert {:error, %Error{kind: :data_invalid, message: "duplicate column id"}} =
               Table.append(table, dup)
    end

    test "floats are cast to integer columns only without losing a fraction", %{table: table} do
      assert {:error, %Error{kind: :data_invalid, message: message}} =
               Table.append(table, DF.new(%{"id" => [1.0, 1.5]}))

      assert message =~ "column id: 1.5 cannot be stored without losing precision"

      assert {:ok, table} = Table.append(table, DF.new(%{"id" => [2.0, 3.0]}))
      {:ok, ipc} = Table.scan(table)
      assert ids(ipc) == [2, 3]
    end

    test "file prefixes must be a plain, non-empty file name", %{table: table} do
      for prefix <- ["", "../escaped", "a/b", "a\\b"] do
        assert_raise ArgumentError, ~r/file_prefix/, fn ->
          Table.write_data_files(table, events([1]), file_prefix: prefix)
        end
      end
    end

    test "the snapshot's operation cannot be overridden", %{table: table} do
      assert_raise ArgumentError, ~r/"operation" is set by the commit/, fn ->
        Table.append(table, events([1]), snapshot_properties: %{"operation" => "delete"})
      end
    end
  end

  describe "scans" do
    test "deeply nested filters are refused without crashing the VM", %{table: table} do
      {:ok, table} = Table.append(table, events([1]))
      assert {:ok, _} = Table.scan(table, filter: nest(127, {:==, "id", 1}, &{:not, &1}))

      deep = nest(100_000, {:==, "id", 1}, &{:not, &1})

      assert_raise ArgumentError, ~r/nested more than 128 levels/, fn ->
        Table.scan(table, filter: deep)
      end

      # The NIF guards itself too.
      assert_raise ArgumentError, ~r/nested more than 128 levels/, fn ->
        Native.call([], &Native.table_scan(&1, &2, table.resource, nil, deep, nil, nil, true))
      end
    end

    test "filter values must fit the NIF", %{table: table} do
      assert_raise ArgumentError, ~r/64-bit/, fn ->
        Table.scan(table, filter: {:==, "id", 2 ** 64})
      end

      assert_raise ArgumentError, ~r/UTF-8/, fn ->
        Table.scan(table, filter: {:==, "name", <<0xFF>>})
      end
    end

    test "select is a list of distinct column names", %{table: table} do
      assert_raise ArgumentError, ~r/:select/, fn -> Table.scan(table, select: "id") end
      assert_raise ArgumentError, ~r/:select/, fn -> Table.scan(table, select: ["id", "id"]) end
    end

    test "timeouts are positive integers or :infinity", %{table: table} do
      for timeout <- [0, -1, "1"] do
        assert_raise ArgumentError, ~r/:timeout/, fn -> Table.scan(table, timeout: timeout) end
      end
    end
  end

  test "deeply nested schemas are refused without crashing the VM", %{catalog: catalog} do
    deep = %{"type" => "struct", "fields" => nest(100_000, "x", &[&1])}

    assert_raise ArgumentError, ~r/nested more than 128 levels/, fn ->
      Catalog.create_table(catalog, "analytics", "deep", deep)
    end
  end
end
