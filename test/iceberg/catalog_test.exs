defmodule Iceberg.CatalogTest do
  use ExUnit.Case, async: true

  import Iceberg.TestHelpers
  alias Iceberg.{Catalog, Error, Table}

  @moduletag :tmp_dir

  test "tables can be created and loaded", %{tmp_dir: dir} do
    catalog = catalog(dir)

    assert {:ok, %Table{namespace: ["analytics"], name: "events"}} =
             Catalog.create_table(catalog, "analytics", "events", events_schema(),
               properties: %{"write.parquet.compression-codec" => "zstd"}
             )

    assert {:error, %Error{kind: :table_already_exists}} =
             Catalog.create_table(catalog, "analytics", "events", events_schema())

    assert {:ok, table} = Catalog.load_table(catalog, ["analytics"], "events")
    metadata = Table.metadata(table)
    assert metadata["properties"]["write.parquet.compression-codec"] == "zstd"
    assert metadata["format-version"] == 2

    assert {:error, %Error{kind: :table_not_found, retryable: false}} =
             Catalog.load_table(catalog, "analytics", "missing")

    assert {:error, %Error{kind: :namespace_already_exists}} =
             Catalog.create_namespace(catalog, "analytics")
  end

  test "namespaces can be created, listed, read and dropped", %{tmp_dir: dir} do
    catalog = catalog(dir)

    assert :ok = Catalog.create_namespace(catalog, ["analytics", "web"], %{"owner" => "ty"})
    assert {:ok, [["analytics"]]} = Catalog.list_namespaces(catalog)
    assert {:ok, [["analytics", "web"]]} = Catalog.list_namespaces(catalog, "analytics")
    assert {:ok, %{"owner" => "ty"}} = Catalog.get_namespace(catalog, ["analytics", "web"])
    assert {:ok, true} = Catalog.namespace_exists(catalog, ["analytics", "web"])

    assert :ok = Catalog.drop_namespace(catalog, ["analytics", "web"])
    assert {:ok, false} = Catalog.namespace_exists(catalog, ["analytics", "web"])

    assert {:error, %Error{kind: :namespace_not_found}} = Catalog.get_namespace(catalog, "nope")
  end

  test "tables can be listed, renamed and dropped", %{tmp_dir: dir} do
    catalog = catalog(dir)
    {:ok, _} = Catalog.create_table(catalog, "analytics", "events", events_schema())

    assert {:ok, ["events"]} = Catalog.list_tables(catalog, "analytics")
    assert {:ok, true} = Catalog.table_exists(catalog, "analytics", "events")

    assert :ok = Catalog.rename_table(catalog, {"analytics", "events"}, {"analytics", "clicks"})
    assert {:ok, ["clicks"]} = Catalog.list_tables(catalog, "analytics")

    assert :ok = Catalog.drop_table(catalog, "analytics", "clicks")
    assert {:ok, false} = Catalog.table_exists(catalog, "analytics", "clicks")
  end

  test "unknown catalog kinds and bad arguments are reported" do
    assert {:error, %Error{kind: :feature_unsupported}} = Catalog.new(:nope, %{})
    assert_raise ArgumentError, fn -> Catalog.new(:memory, %{"warehouse" => 1}) end
  end

  test "a REST catalog connects on first use and reports what it cannot reach" do
    {:ok, catalog} = Catalog.new(:rest, %{"uri" => "http://127.0.0.1:1", "warehouse" => "w"})

    assert {:error, %Error{kind: :unexpected}} =
             Catalog.load_table(catalog, "analytics", "events", timeout: 10_000)
  end

  test "calls past their timeout return a timeout error" do
    # A catalog that accepts connections and never answers.
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, _socket} = :gen_tcp.accept(listen)
      Process.sleep(:infinity)
    end)

    {:ok, catalog} =
      Catalog.new(:rest, %{"uri" => "http://127.0.0.1:#{port}", "warehouse" => "w"})

    assert {:error, %Error{kind: :timeout, retryable: true}} =
             Catalog.load_table(catalog, "analytics", "events", timeout: 100)
  end
end
