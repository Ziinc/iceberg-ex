defmodule Iceberg.DataFiles do
  @moduledoc """
  Data files written by `Iceberg.Table.write_data_files/3` and not yet
  committed. Pass them to `Iceberg.Table.fast_append/3`.
  """
  @enforce_keys [:resource, :files]
  defstruct [:resource, :files]

  @type file :: %{
          path: String.t(),
          record_count: non_neg_integer(),
          file_size_in_bytes: non_neg_integer()
        }
  @type t :: %__MODULE__{resource: reference(), files: [file()]}
end
