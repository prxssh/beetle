defmodule Beetle.ConfigTest do
  use ExUnit.Case

  test "starts and provides access to configuration values" do
    mock_config = %{
      port: 5432,
      log_file_size: 1_024_000,
      merge_interval: 300,
      database_shards: 4,
      storage_directory: "/tmp/beetle/data",
      log_rotation_interval: 3600
    }

    Agent.update(Beetle.Config, fn _ -> mock_config end)

    assert Beetle.Config.port() == mock_config.port
    assert Beetle.Config.log_file_size() == mock_config.log_file_size
    assert Beetle.Config.merge_interval() == mock_config.merge_interval
    assert Beetle.Config.database_shards() == mock_config.database_shards
    assert Beetle.Config.storage_directory() == mock_config.storage_directory
    assert Beetle.Config.log_rotation_interval() == mock_config.log_rotation_interval
  end
end
