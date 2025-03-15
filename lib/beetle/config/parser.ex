defmodule Beetle.Config.Parser do
  @moduledoc """
  Module responsible for reading and parsing the configuration file for the
  Beetle database. 

  It handles reading a config file from disk, parsing its contents, and
  validating configuration values. The configuration file uses a simple
  key-value format where each line contains a key and value separated by space.
  Comments start with `#` and empty lines are ignored.

  ## Configuration Options

  * `log_file_size` - Maximum size of log file (in bytes)
  * `port` - TCP port on which Beetle database server listens
  * `storage_directory` - Directory where storage files are stored
  * `log_rotation_interval` - Time in seconds between log rotations
  * `database_shards` - Number of database shards for parallel processing
  * `merge_interval` - Time in seconds between background merge operations
  """
  import Beetle.Utils

  @type t :: %__MODULE__{
          port: pos_integer(),
          log_file_size: pos_integer(),
          merge_interval: pos_integer(),
          storage_directory: String.t(),
          database_shards: pos_integer(),
          log_rotation_interval: pos_integer()
        }

  defstruct(
    port: 6969,
    log_file_size: 5 * 1024 * 1024,
    merge_interval: :timer.minutes(30),
    log_rotation_interval: :timer.minutes(30),
    database_shards: System.schedulers_online(),
    storage_directory: Path.expand("~/.local/share/beetle")
  )

  @spec read_config(nil | String.t()) :: t()
  def read_config(nil), do: %__MODULE__{}

  def read_config(path) do
    case :file.read_file(path) do
      {:ok, content} -> parse_config(content)
      {:error, reason} -> raise reason
    end
  end

  # === Private

  @spec parse_config(String.t()) :: t()
  defp parse_config(content) do
    content
    |> String.split("\n")
    |> Stream.reject(fn val -> String.starts_with?(val, "#") or String.trim(val) == "" end)
    |> Enum.reduce(%__MODULE__{}, &parse_line/2)
  end

  @spec parse_line(String.t(), t()) :: t()
  defp parse_line(line, config) do
    case String.split(line, " ", parts: 2) do
      [key, value] ->
        key = key |> String.trim() |> String.to_atom()
        value = value |> String.trim()

        update_config(config, key, value)

      _ ->
        config
    end
  end

  @spec update_config(t(), atom(), String.t()) :: t()
  defp update_config(config, :port, value) do
    case parse_integer(value) do
      {:ok, port} -> %__MODULE__{config | port: port}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :storage_directory, value) do
    path = Path.expand(value)
    updated_config = %{config | storage_directory: path}

    case :filelib.ensure_dir(path) do
      :ok -> updated_config
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :database_shards, value) do
    case parse_integer(value) do
      {:ok, shards} -> %__MODULE__{config | database_shards: shards}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :log_rotation_interval, value) do
    case parse_time(value) do
      {:ok, time} -> %__MODULE__{config | log_rotation_interval: time}
      {:error, reason} -> raise reason
    end
  end

  defp merge_interval(config, :merge_interval, value) do
    case parse_time(value) do
      {:ok, time} -> %__MODULE__{config | merge_interval: time}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :log_file_size, value) do
    case parse_file_size(value) do
      {:ok, size_mb} -> %__MODULE__{config | log_file_size: size_mb}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, _, _), do: config

  @spec parse_time(String.t()) :: {:ok, pos_integer()} | {:error, String.t()}
  defp parse_time(str) do
    case Regex.run(~r/^(\d+)([smh])$/i, str) do
      [_, num_str, unit] ->
        case parse_integer(num_str) do
          {:ok, time} -> {:ok, parse_time_unit(time, String.downcase(unit))}
          error -> error
        end

      _ ->
        {:error, "Invalid format, expected 'time<unit>'"}
    end
  end

  defp parse_time_unit(value, "s"), do: value
  defp parse_time_unit(value, "m"), do: value * 60
  defp parse_time_unit(value, "h"), do: value * 3600
  defp parse_time_unit(_, unit), do: raise("Invalid time '#{unit}'")

  @spec parse_file_size(String.t()) :: {:ok, pos_integer()} | {:error, String.t()}
  defp parse_file_size(str) do
    case Regex.run(~r/^(\d+)([kmgMG][bB])$/i, str) do
      [_, num_str, unit] ->
        case parse_integer(num_str) do
          {:ok, size} -> {:ok, parse_file_size_unit(size, String.downcase(unit))}
          error -> error
        end

      _ ->
        {:error, "Invalid format, expected 'size<unit>'"}
    end
  end

  defp parse_file_size_unit(value, "kb"), do: value * 1024
  defp parse_file_size_unit(value, "mb"), do: value * 1024 * 1024
  defp parse_file_size_unit(value, "gb"), do: value * 1024 * 1024 * 1024
  defp parse_file_size_unit(_, unit), do: raise("Invalid file size '#{unit}'")
end
