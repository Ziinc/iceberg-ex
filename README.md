# Iceberg

[Apache Iceberg](https://iceberg.apache.org) tables for Elixir, backed by
[iceberg-rust](https://github.com/apache/iceberg-rust) 0.11 through a precompiled
Rustler NIF.

- Catalogs: Iceberg REST, Amazon S3 Tables, in-memory
- Namespaces and tables: create (partitioned or not), list, load, rename, drop
- Scans with column selection, row filters and time travel, as Arrow IPC or an
  `Explorer.DataFrame`
- Appends: Parquet data files written separately from the commit, then
  fast-appended with custom snapshot summary properties

Calls run on the NIF's own Tokio runtime and reply by message, so long catalog
or storage requests never hold a BEAM scheduler.

## Installation

```elixir
def deps do
  [
    {:iceberg, "~> 0.1"},
    # optional: append DataFrames and scan into them
    {:explorer, "~> 0.11"}
  ]
end
```

Precompiled NIFs cover macOS and Linux (glibc and musl) on x86_64 and aarch64,
and Windows on x86_64. Elsewhere, add `{:rustler, "~> 0.38"}` and build from source with
`RUSTLER_PRECOMPILED_FORCE_BUILD_ALL=1`.

## Usage

```elixir
alias Explorer.DataFrame, as: DF

{:ok, catalog} =
  Iceberg.Catalog.new(:rest, %{
    "uri" => "https://catalog.example.com",
    "warehouse" => "analytics",
    "token" => token,
    "s3.region" => "eu-west-1"
  })

:ok = Iceberg.Catalog.create_namespace(catalog, "web")

schema =
  Iceberg.Schema.new([
    {"id", :long, required: true},
    {"name", :string},
    {"ts", :timestamptz}
  ])

{:ok, table} =
  Iceberg.Catalog.create_table(catalog, "web", "events", schema, partition_by: [{"ts", :day}])

# Append: data is an Arrow IPC stream binary or an Explorer.DataFrame.
{:ok, table} = Iceberg.Table.append(table, DF.new(%{"id" => [1, 2], "name" => ["a", "b"]}))

# Scan
{:ok, ipc} = Iceberg.Table.scan(table, select: ["id"], filter: {:>, "id", 1})
{:ok, df} = Iceberg.Table.to_dataframe(table, filter: {:==, "name", "a"})

# Write files now, commit later (or from one process per table).
{:ok, files} = Iceberg.Table.write_data_files(table, df, file_prefix: "batch-42")
{:ok, table} = Iceberg.Table.fast_append(table, files, snapshot_properties: %{"app.batch" => "42"})

Iceberg.Table.metadata(table)["snapshots"]
```

## Development

```bash
mix deps.get
mix test
```

The NIF is built from `native/iceberg_nif` in `:dev` and `:test`.

## Releasing

1. Bump the version in `mix.exs`, tag `vX.Y.Z` and push the tag. The `Release NIFs`
   workflow attaches the NIFs to the GitHub release.
2. `mix rustler_precompiled.download Iceberg.Native --all --print` writes
   `checksum-Elixir.Iceberg.Native.exs`.
3. `mix hex.publish`.
