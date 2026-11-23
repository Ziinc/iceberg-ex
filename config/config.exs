import Config

# Build the NIF from source when developing this library; consumers download
# the precompiled artifact (or set ICEBERG_BUILD=1).
if config_env() in [:dev, :test] do
  config :rustler_precompiled, :force_build, iceberg: true
end
