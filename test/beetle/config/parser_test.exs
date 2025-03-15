defmodule Beetle.Config.ParserTest do
  use ExUnit.Case, async: true

  alias Beetle.Config.Parser

  @example_config_path "example/beetle.conf"
  @temp_dir Path.join(System.tmp_dir!(), "beetle_test_#{System.unique_integer([:positive])}")

  setup do
    File.mkdir_p!(@temp_dir)

    on_exit(fn -> File.rm_rf!(@temp_dir) end)
    {:ok, temp_dir: @temp_dir}
  end

  describe "read_config/1" do
    test "returns default configuration file when nil is provided" do
      config = Parser.read_config(nil)

      assert config.port == 6969
      assert config.log_file_size == 5 * 1024 * 1024
      assert config.merge_interval == :timer.minutes(30)
      assert config.log_rotation_interval == :timer.minutes(30)
      assert config.database_shards == System.schedulers_online()
      assert config.storage_directory == Path.expand("~/.local/share/beetle")
    end

    test "reads and parses the example configuration file" do
      config = Parser.read_config(@example_config_path)

      assert config.port == 5555
      assert config.database_shards == 1
      assert config.merge_interval == 60
      assert config.log_rotation_interval == 60
      assert config.log_file_size == 1 * 1024 * 1024
      assert config.storage_directory == Path.expand("./example/db")
    end

    test "raises error on nonexistent file path" do
      assert_raise UndefinedFunctionError, fn ->
        Parser.read_config("nonexistent/config/path.conf")
      end
    end

    test "raises error on invalid port value", %{temp_dir: temp_dir} do
      path = Path.join(temp_dir, "invalid_port.conf")
      File.write!(path, "port invalid")

      assert_raise RuntimeError, fn -> Parser.read_config(path) end
    end
  end

  test "raises error on invalid file size format", %{temp_dir: temp_dir} do
    path = Path.join(temp_dir, "invalid_size.conf")
    File.write!(path, "log_file_size 10ZB")

    assert_raise RuntimeError, fn -> Parser.read_config(path) end
  end

  test "raises error on invalid time format", %{temp_dir: temp_dir} do
    path = Path.join(temp_dir, "invalid_time.conf")
    File.write!(path, "log_rotation_interval 10y")

    assert_raise RuntimeError, fn -> Parser.read_config(path) end
  end

  test "ignores comments and empty lines", %{temp_dir: temp_dir} do
    path = Path.join(temp_dir, "comments.conf")

    content = """
    # This is a comment

    port 8080

    # Another comment
    log_file_size 2MB
    """

    File.write!(path, content)

    config = Parser.read_config(path)
    assert config.port == 8080
    assert config.log_file_size == 2 * 1024 * 1024
  end

  test "handles various time units in configuration", %{temp_dir: temp_dir} do
    path = Path.join(temp_dir, "time_units.conf")

    content = """
    log_rotation_interval 10s
    """

    File.write!(path, content)

    config = Parser.read_config(path)
    # 10 seconds
    assert config.log_rotation_interval == 10
  end

  test "handles various file size units in configuration", %{temp_dir: temp_dir} do
    # Test KB
    kb_path = Path.join(temp_dir, "file_sizes_kb.conf")
    File.write!(kb_path, "log_file_size 2KB")

    kb_config = Parser.read_config(kb_path)
    # 2 KB
    assert kb_config.log_file_size == 2 * 1024

    # Test GB
    gb_path = Path.join(temp_dir, "file_sizes_gb.conf")
    File.write!(gb_path, "log_file_size 1GB")

    gb_config = Parser.read_config(gb_path)
    # 1 GB
    assert gb_config.log_file_size == 1 * 1024 * 1024 * 1024
  end

  test "ensures storage directory exists", %{temp_dir: temp_dir} do
    path = Path.join(temp_dir, "storage_dir.conf")
    dir_path = Path.join(temp_dir, "beetle_storage_test")
    content = "storage_directory #{dir_path}"
    File.write!(path, content)

    config = Parser.read_config(path)
    assert config.storage_directory == Path.expand(dir_path)
  end

  test "ignores unknown configuration keys", %{temp_dir: temp_dir} do
    path = Path.join(temp_dir, "unknown_keys.conf")

    content = """
    port 8080
    unknown_setting value
    """

    File.write!(path, content)

    config = Parser.read_config(path)
    assert config.port == 8080
    assert config.log_file_size == 5 * 1024 * 1024
  end
end
