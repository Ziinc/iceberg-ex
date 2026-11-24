defmodule Iceberg.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/Ziinc/iceberg-ex"

  def project do
    [
      app: :iceberg,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Apache Iceberg tables for Elixir, backed by iceberg-rust",
      package: package(),
      docs: [
        main: "readme",
        extras: ["README.md"],
        source_ref: "v#{@version}",
        source_url: @source_url
      ]
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:rustler_precompiled, "~> 0.8"},
      {:rustler, "~> 0.38", optional: true},
      {:explorer, "~> 0.11", optional: true},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(
        lib mix.exs README.md LICENSE checksum-*.exs
        native/iceberg_nif/src native/iceberg_nif/.cargo
        native/iceberg_nif/Cargo.toml native/iceberg_nif/Cargo.lock
      )
    ]
  end
end
