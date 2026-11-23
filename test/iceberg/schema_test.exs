defmodule Iceberg.SchemaTest do
  use ExUnit.Case, async: true

  test "assigns field ids to top-level fields first, then nested ones" do
    assert Iceberg.Schema.new([
             {"id", :long, required: true, doc: "row id"},
             {"tags", {:list, :string}},
             {"attrs", {:map, :string, :int}},
             {"point", {:struct, [{"x", :double, required: true}, {"y", :double}]}},
             {"price", {:decimal, 10, 2}},
             {"hash", {:fixed, 16}}
           ]) == %{
             "type" => "struct",
             "schema-id" => 0,
             "fields" => [
               %{
                 "id" => 1,
                 "name" => "id",
                 "required" => true,
                 "type" => "long",
                 "doc" => "row id"
               },
               %{
                 "id" => 2,
                 "name" => "tags",
                 "required" => false,
                 "type" => %{
                   "type" => "list",
                   "element-id" => 7,
                   "element" => "string",
                   "element-required" => false
                 }
               },
               %{
                 "id" => 3,
                 "name" => "attrs",
                 "required" => false,
                 "type" => %{
                   "type" => "map",
                   "key-id" => 8,
                   "key" => "string",
                   "value-id" => 9,
                   "value" => "int",
                   "value-required" => false
                 }
               },
               %{
                 "id" => 4,
                 "name" => "point",
                 "required" => false,
                 "type" => %{
                   "type" => "struct",
                   "fields" => [
                     %{"id" => 10, "name" => "x", "required" => true, "type" => "double"},
                     %{"id" => 11, "name" => "y", "required" => false, "type" => "double"}
                   ]
                 }
               },
               %{"id" => 5, "name" => "price", "required" => false, "type" => "decimal(10, 2)"},
               %{"id" => 6, "name" => "hash", "required" => false, "type" => "fixed[16]"}
             ]
           }
  end

  test "rejects unknown types" do
    assert_raise ArgumentError, ~r/unknown Iceberg type :text/, fn ->
      Iceberg.Schema.new([{"a", :text}])
    end
  end
end
