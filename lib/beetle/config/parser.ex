defmodule Beetle.Config.Parser do
  @moduledoc """
  Parses the configuration file into an Elixir struct.

  This module handles parsing of configuration file into a struct. It reads the
  configuration file from path, or initializes a default configuration.

  The configuration file is a **line-based** text file containing key/value
  pairs separated by a whitespace. Lines beginning with `#` are considered
  comments, and ignored during parsing. Blank lines are also ignored.

  A sample config file is provided in [here](beetle/data/beetle.conf).
  """

  @typedoc """
  Struct representing the database configuration. The following configuration
  keys are supported:

  - `port`: TCP port on which database is listenting for connections
  - `storage_directory`: Directory where database files are stored
  - `log_file_size`: Maximum allowed size of Bitcask log file (in bytes). Once
    the size limit is breached, it'll be rotated.
  - `merge_interval`: Interval (in seconds) after which Bitcask merge operation
    is triggered.
  - `log_rotation_interval`: Interval (in seconds) after which Bitcask log
    rotation operation is triggered.
  """
  require Logger
  import Beetle.Utils

  @type t :: %__MODULE__{
          port: pos_integer(),
          log_file_size: pos_integer(),
          storage_directory: String.t(),
          database_shards: pos_integer(),
          merge_interval: pos_integer() | nil,
          log_rotation_interval: pos_integer() | nil
        }

  defstruct(
    port: 6969,
    log_file_size: 5 * 1024 * 1024,
    merge_interval: :timer.minutes(30),
    log_rotation_interval: :timer.minutes(10),
    database_shards: System.schedulers_online(),
    storage_directory: Path.expand("~/.local/share/beetle")
  )

  @doc """
  Reads the configuration stored at `path` and serializes it into a struct.

  If no configuration is available, it initializes a default configuration for
  the database.
  """
  @spec read_config(nil | String.t()) :: t()
  def read_config(nil), do: %__MODULE__{}

  def read_config(path) do
    case File.read(path) do
      {:ok, data} ->
        parse_config(data)

      {:error, reason} ->
        Logger.error("#{__MODULE__}: failed to read config at `#{path}`: #{inspect(reason)}")
        %__MODULE__{}
    end
  end

  ############### Private

  @spec parse_config(binary()) :: t()
  defp parse_config(data) do
    data
    |> String.split("\n")
    |> Stream.reject(fn line -> String.starts_with?(line, "#") or String.trim(line) == "" end)
    |> Enum.reduce(%__MODULE__{}, &parse_line/2)
  end

  @spec parse_line(binary(), t()) :: t()
  defp parse_line(line, config) do
    case String.split(line, " ", parts: 2) do
      [key, value] ->
        key = key |> String.trim() |> String.downcase() |> String.to_atom()
        value = String.trim(value)

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
    updated_config = %__MODULE__{config | storage_directory: path}

    if File.exists?(path),
      do: updated_config,
      else: raise("storage directory not present at '#{value}'")
  end

  defp update_config(config, :database_shards, value) do
    case parse_integer(value) do
      {:ok, shards} -> %__MODULE__{config | database_shards: shards}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :log_rotation_interval, "SKIP"),
    do: %__MODULE__{config | log_rotation_interval: nil}

  defp update_config(config, :log_rotation_interval, value) do
    case parse_time(value) do
      {:ok, interval_sec} -> %__MODULE__{config | log_rotation_interval: interval_sec}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :merge_interval, "SKIP"),
    do: %__MODULE__{config | merge_interval: nil}

  defp update_config(config, :merge_interval, value) do
    case parse_time(value) do
      {:ok, interval_sec} -> %__MODULE__{config | merge_interval: interval_sec}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :log_file_size, value) do
    case parse_file_size(value) do
      {:ok, size_bytes} -> %__MODULE__{config | log_file_size: size_bytes}
      {:error, reason} -> raise reason
    end
  end

  defp parse_time(str) do
    case Regex.run(~r/^(\d+)([smh])$/i, str) do
      [_, val_str, unit] ->
        case parse_integer(val_str) do
          {:ok, time} -> parse_time_unit(time, String.downcase(unit))
          error -> error
        end

      _ ->
        {:error, "Invalid format, expected 'time<unit>'"}
    end
  end

  defp parse_time_unit(value, "s"), do: {:ok, value}
  defp parse_time_unit(value, "m"), do: {:ok, value * 60}
  defp parse_time_unit(value, "h"), do: {:ok, value * 3600}
  defp parse_time_unit(_, unit), do: {:error, "Invalid time '#{unit}'"}

  @spec parse_file_size(String.t()) :: {:ok, pos_integer()} | {:error, String.t()}
  defp parse_file_size(str) do
    case Regex.run(~r/^(\d+)([kmgMG][bB])$/i, str) do
      [_, num_str, unit] ->
        case parse_integer(num_str) do
          {:ok, size} -> parse_file_size_unit(size, String.downcase(unit))
          error -> error
        end

      _ ->
        {:error, "Invalid format, expected 'size<unit>'"}
    end
  end

  defp parse_file_size_unit(value, "kb"), do: {:ok, value * 1024}
  defp parse_file_size_unit(value, "mb"), do: {:ok, value * 1024 * 1024}
  defp parse_file_size_unit(value, "gb"), do: {:ok, value * 1024 * 1024 * 1024}
  defp parse_file_size_unit(_, unit), do: {:error, "Invalid file size '#{unit}'"}
end
