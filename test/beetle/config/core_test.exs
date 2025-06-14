defmodule Beetle.Config.CoreTest do
  use ExUnit.Case, async: true

  alias Beetle.Config

  setup do
    # Create a temporary directory for testing
    tmp_dir = Path.join(System.tmp_dir(), "beetle_config_test_#{:rand.uniform(1000)}")
    File.mkdir_p!(tmp_dir)

    # Stop any running Config agents to avoid conflicts between tests
    try do
      Agent.stop(Config)
    catch
      :exit, _ -> :ok
    end

    on_exit(fn ->
      # Clean up temporary files
      File.rm_rf!(tmp_dir)

      # Stop the Config agent
      try do
        Agent.stop(Config)
      catch
        :exit, _ -> :ok
      end
    end)

    %{tmp_dir: tmp_dir}
  end

  describe "start_link/1" do
    test "starts with nil configuration" do
      assert {:ok, _pid} = Config.start_link(nil)

      # Check default values
      assert Config.port() == 6969
      assert Config.log_file_size() == 5 * 1024 * 1024
      assert Config.database_shards() == System.schedulers_online()
      assert Config.storage_directory() == Path.expand("~/.local/share/beetle")
    end

    test "starts with valid configuration file", %{tmp_dir: tmp_dir} do
      # Create a valid test configuration file
      config_content = """
      # Beetle Configuration
      port 7070
      storage_directory #{tmp_dir}
      log_file_size 10MB
      database_shards 8
      merge_interval 1h
      log_rotation_interval 30m
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      assert {:ok, _pid} = Config.start_link(config_path)

      # Verify all config values
      assert Config.port() == 7070
      assert Config.log_file_size() == 10 * 1024 * 1024
      assert Config.database_shards() == 8
      assert Config.storage_directory() == tmp_dir
    end

    test "starts with nonexistent file path" do
      assert {:ok, _pid} = Config.start_link("/nonexistent/path.conf")

      # Should use default values
      assert Config.port() == 6969
      assert Config.log_file_size() == 5 * 1024 * 1024
    end
  end

  describe "accessor functions" do
    setup %{tmp_dir: tmp_dir} do
      # Create a test configuration file with all settings
      config_content = """
      port 8080
      storage_directory #{tmp_dir}
      log_file_size 20MB
      database_shards 4
      merge_interval 2h
      log_rotation_interval 15m
      """

      config_path = Path.join(tmp_dir, "full_config.conf")
      File.write!(config_path, config_content)

      {:ok, _pid} = Config.start_link(config_path)

      :ok
    end

    test "port/0 returns the configured port" do
      assert Config.port() == 8080
    end

    test "log_file_size/0 returns the configured log file size" do
      assert Config.log_file_size() == 20 * 1024 * 1024
    end

    test "merge_interval/0 returns the configured merge interval" do
      # Note: There might be a discrepancy in the field name (merge_interval vs merge_internal)
      # This test assumes the implementation correctly maps between them
      # 2 hours in seconds
      assert Config.merge_interval() == 7200
    end

    test "log_rotation_interval/0 returns the configured log rotation interval" do
      # 15 minutes in seconds
      assert Config.log_rotation_interval() == 900
    end

    test "database_shards/0 returns the configured database shards" do
      assert Config.database_shards() == 4
    end

    test "storage_directory/0 returns the configured storage directory", %{tmp_dir: tmp_dir} do
      assert Config.storage_directory() == tmp_dir
    end
  end

  describe "accessor functions with SKIP values" do
    setup %{tmp_dir: tmp_dir} do
      # Create a test configuration with SKIP intervals
      config_content = """
      storage_directory #{tmp_dir}
      merge_interval SKIP
      log_rotation_interval SKIP
      """

      config_path = Path.join(tmp_dir, "skip_config.conf")
      File.write!(config_path, config_content)

      {:ok, _pid} = Config.start_link(config_path)

      :ok
    end

    test "merge_interval/0 returns nil when SKIP is specified" do
      assert Config.merge_interval() == nil
    end

    test "log_rotation_interval/0 returns nil when SKIP is specified" do
      assert Config.log_rotation_interval() == nil
    end
  end

  describe "concurrent access" do
    setup %{tmp_dir: tmp_dir} do
      config_path = Path.join(tmp_dir, "concurrent.conf")
      File.write!(config_path, "port 9090\nstorage_directory #{tmp_dir}")

      {:ok, _pid} = Config.start_link(config_path)

      :ok
    end

    test "allows concurrent access to configuration values" do
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            assert Config.port() == 9090
            assert is_binary(Config.storage_directory())
          end)
        end

      # All tasks should complete successfully
      Enum.each(tasks, &Task.await/1)
    end
  end

  describe "error handling" do
    test "handles invalid configuration file gracefully", %{tmp_dir: tmp_dir} do
      # Create a test configuration with an error
      config_content = """
      port invalid_port
      storage_directory #{tmp_dir}
      """

      config_path = Path.join(tmp_dir, "invalid_config.conf")
      File.write!(config_path, config_content)

      # The start_link call should fail because of the error in the config
      assert_raise RuntimeError, fn ->
        Config.start_link(config_path)
      end
    end
  end
end
