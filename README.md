# Iceberg

[Apache Iceberg](https://iceberg.apache.org) tables for Elixir, backed by
[iceberg-rust](https://github.com/apache/iceberg-rust) 0.11 through a precompiled
Rustler NIF.

- Catalogs: Iceberg REST, Amazon S3 Tables, in-memory
- Namespaces and tables: create (partitioned or not), list, load, rename, drop
- Scans with column selection, row filters and time travel, as Arrow IPC or an
  `Explorer.DataFrame`, and row counts
- Appends from columns (a map of lists), Arrow IPC or an `Explorer.DataFrame`:
  Parquet data files written separately from the commit, then fast-appended
  with custom snapshot summary properties

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

# Append: data is columns, an Arrow IPC stream binary or an Explorer.DataFrame.
{:ok, table} = Iceberg.Table.append(table, %{"id" => [1, 2], "name" => ["a", nil]})
{:ok, table} = Iceberg.Table.append(table, DF.new(%{"id" => [3], "name" => ["c"]}))

# Scan
{:ok, ipc} = Iceberg.Table.scan(table, select: ["id"], filter: {:>, "id", 1})
{:ok, df} = Iceberg.Table.to_dataframe(table, filter: {:==, "name", "a"})
{:ok, 1} = Iceberg.Table.count(table, filter: {:==, "name", "a"})

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

1. Merge a version bump of `@version` in `mix.exs`.
2. Tag the merge `vX.Y.Z` and push the tag. The `Release` workflow then:
   - builds the NIFs and attaches them to the GitHub release;
   - generates `checksum-Elixir.Iceberg.Native.exs` from those files,
     attaches it to the release and commits it to `main`;
   - publishes the package, with the checksum file, and docs to Hex
     (needs the `HEX_API_KEY` repository secret).

To retry a failed release, run the workflow by hand with the existing tag.
