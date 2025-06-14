defmodule Beetle.Config.ParserTest do
  use ExUnit.Case, async: true

  alias Beetle.Config.Parser, as: ConfigParser

  setup do
    tmp_dir = Path.join(System.tmp_dir(), "beetle_test_#{:rand.uniform(1000)}")
    File.mkdir_p!(tmp_dir)

    on_exit(fn -> File.rm_rf!(tmp_dir) end)

    %{tmp_dir: tmp_dir}
  end

  describe "read_config/1" do
    test "returns default configuration when nil is provided" do
      config = ConfigParser.read_config(nil)

      assert config.port == 6969
      assert config.log_file_size == 5 * 1024 * 1024
      assert config.database_shards == System.schedulers_online()
      assert config.merge_interval == :timer.minutes(30)
      assert config.log_rotation_interval == :timer.minutes(10)
      assert config.storage_directory == Path.expand("~/.local/share/beetle")
    end

    test "returns default configuration when file does not exist" do
      config = ConfigParser.read_config("/nonexistent/file.conf")

      assert config.port == 6969
      assert config.log_file_size == 5 * 1024 * 1024
      assert config.database_shards == System.schedulers_online()
      assert config.merge_interval == :timer.minutes(30)
      assert config.log_rotation_interval == :timer.minutes(10)
      assert config.storage_directory == Path.expand("~/.local/share/beetle")
    end

    test "parses valid configuration file", %{tmp_dir: tmp_dir} do
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

      # Test the configuration
      config = ConfigParser.read_config(config_path)

      assert config.port == 7070
      assert config.log_file_size == 10 * 1024 * 1024
      assert config.database_shards == 8
      # 1h in seconds
      assert config.merge_interval == 3600
      # 30m in seconds
      assert config.log_rotation_interval == 1800
      assert config.storage_directory == tmp_dir
    end

    test "sets intervals to nil when SKIP is specified", %{tmp_dir: tmp_dir} do
      # Create a test configuration file with SKIP values
      config_content = """
      storage_directory #{tmp_dir}
      merge_interval SKIP
      log_rotation_interval SKIP
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      # Test the configuration
      config = ConfigParser.read_config(config_path)

      assert config.merge_interval == nil
      assert config.log_rotation_interval == nil
    end

    test "raises error for invalid port configuration", %{tmp_dir: tmp_dir} do
      # Create a test configuration file with invalid port
      config_content = """
      port invalid
      storage_directory #{tmp_dir}
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      # Test the configuration raises error
      assert_raise RuntimeError, fn ->
        ConfigParser.read_config(config_path)
      end
    end

    test "raises error for nonexistent storage directory", %{tmp_dir: tmp_dir} do
      nonexistent_dir = Path.join(tmp_dir, "nonexistent")

      # Create a test configuration file with nonexistent directory
      config_content = """
      storage_directory #{nonexistent_dir}
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      # Test the configuration raises error
      assert_raise RuntimeError, fn ->
        ConfigParser.read_config(config_path)
      end
    end

    test "parses time intervals in seconds", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      log_rotation_interval 60s
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)
      assert config.log_rotation_interval == 60
    end

    test "parses time intervals in minutes", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      log_rotation_interval 5m
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)
      # 5 minutes in seconds
      assert config.log_rotation_interval == 300
    end

    test "parses time intervals in hours", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      merge_interval 2h
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)
      # 2 hours in seconds
      assert config.merge_interval == 7200
    end

    test "raises error for invalid time unit", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      merge_interval 10d
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      assert_raise RuntimeError, fn ->
        ConfigParser.read_config(config_path)
      end
    end

    test "parses file size in KB", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      log_file_size 512KB
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)
      assert config.log_file_size == 512 * 1024
    end

    test "parses file size in MB", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      log_file_size 2MB
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)
      assert config.log_file_size == 2 * 1024 * 1024
    end

    test "parses file size in GB", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      log_file_size 1GB
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)
      assert config.log_file_size == 1 * 1024 * 1024 * 1024
    end

    test "raises error for invalid file size unit", %{tmp_dir: tmp_dir} do
      config_content = """
      storage_directory #{tmp_dir}
      log_file_size 10TB
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      assert_raise RuntimeError, fn ->
        ConfigParser.read_config(config_path)
      end
    end

    test "ignores comments and empty lines", %{tmp_dir: tmp_dir} do
      # Create a test configuration file with comments and empty lines
      config_content = """
      # This is a comment
      port 9090

      # Another comment
      storage_directory #{tmp_dir}
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)

      assert config.port == 9090
      assert config.storage_directory == tmp_dir
    end

    test "ignores malformed lines", %{tmp_dir: tmp_dir} do
      # Create a test configuration file with malformed lines
      config_content = """
      port 9090
      malformed_line
      storage_directory #{tmp_dir}
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)

      assert config.port == 9090
      assert config.storage_directory == tmp_dir
    end

    test "handles multiple consecutive spaces between key and value", %{tmp_dir: tmp_dir} do
      config_content = """
      port     9090
      storage_directory     #{tmp_dir}
      """

      config_path = Path.join(tmp_dir, "beetle.conf")
      File.write!(config_path, config_content)

      config = ConfigParser.read_config(config_path)

      assert config.port == 9090
      assert config.storage_directory == tmp_dir
    end
  end
end
