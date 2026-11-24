defmodule Iceberg.Error do
  @moduledoc """
  An error from iceberg-rust or the catalog.

  `kind` is iceberg-rust's `ErrorKind` in snake case (`:table_not_found`,
  `:namespace_already_exists`, `:catalog_commit_conflicts`, `:data_invalid`,
  `:unexpected`, ...), or `:timeout`. `retryable` is iceberg-rust's own verdict.
  """
  defexception [:kind, :message, retryable: false]
end
