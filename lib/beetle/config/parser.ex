defmodule Beetle.Config.Parser do
  @moduledoc """
  Configuration parser for BeetleDB.
  """
  import Beetle.Utils

  @type t :: %__MODULE__{
          port: integer(),
          host: String.t(),
          # milliseconds
          sync_interval: pos_integer(),
          # bytes
          log_file_size: pos_integer(),
          # milliseconds
          merge_interval: pos_integer(),
          storage_directory: String.t()
        }

  # default 5MB in bytes
  @default_log_size 5 * 1024 * 1024

  defstruct port: 6969,
            host: "127.0.0.1",
            storage_directory: "~/.beetle/",
            sync_interval: :timer.seconds(5),
            log_file_size: @default_log_size,
            merge_interval: :timer.seconds(60)

  @configuration_size "beetle.conf"
  @valid_time_units ["s", "min", "hr"]
  @valid_size_units ["kb", "mb", "gb", "tb"]

  @doc """
  Loads configuration for a file path.

  If file is not found or is invalid, returns default configuration.
  """
  @spec load(String.t()) :: {:ok, t()} | {:error, String.t()}
  def load(path \\ "data/beetle.conf") do
    path
    |> Path.join(@configuration_size)
    |> File.read()
    |> case do
      {:ok, content} -> parse_config(content)
      {:error, :enoent} -> {:ok, load_default_config()}
      {:error, reason} -> {:error, "error loading config: #{inspect(reason)}"}
    end
  end

  @spec load_default_config() :: t()
  defp load_default_config, do: %__MODULE__{}

  @spec parse_config(String.t()) :: t()
  defp parse_config(content) do
    content
    |> String.split("\n")
    |> Enum.reduce(load_default_config(), &parse_line/2)
  end

  @spec parse_line(String.t(), t()) :: t()
  defp parse_line(line, config) do
    line
    |> String.trim()
    |> case do
      "" -> config
      "#" <> _comment -> config
      line -> parse_config_line(line, config)
    end
  end

  @spec parse_config_line(String.t(), t()) :: t()
  defp parse_config_line(line, config) do
    line
    |> String.split(" ", parts: 2)
    |> case do
      [key, value] -> apply_config(String.downcase(key), String.trim(value), config)
      _ -> config
    end
  end

  @spec apply_config(String.t(), String.t(), t()) :: t()
  defp apply_config("port", value, config) do
    case Integer.parse(value) do
      {port, _} -> %{config | port: port}
      :error -> raise "invalid port #{value}"
    end
  end

  defp apply_config("host", value, config), do: %{config | bind: value}
  defp apply_config("storage_directory", value, config), do: %{config | storage_directory: value}

  defp apply_config("merge_interval", value, config) do
    case duration_to_ms(value) do
      {:error, reason} -> raise reason
      duration_ms -> %{config | merge_interval: duration_ms}
    end
  end

  defp apply_config("sync_interval", value, config) do
    case duration_to_ms(value) do
      {:error, reason} -> raise reason
      duration_ms -> %{config | sync_interval: duration_ms}
    end
  end

  defp apply_config("log_file_size", value, config) do
    case size_to_bytes(value) do
      {:error, reason} -> raise reason
      size_bytes -> %{config | log_file_size: size_bytes}
    end
  end

  defp duration_to_ms(duration) do
    case Regex.run(~r/^(\d+)(s|min|hr)$/i, duration) do
      [_, n, unit] ->
        {num, _} = Integer.parse(n)

        case String.downcase(unit) do
          "s" -> :timer.seconds(num)
          "min" -> :timer.minutes(num)
          "hr" -> :timer.minutes(num)
        end

      _ ->
        {:error, "invalid duration format: #{duration}"}
    end
  end

  defp size_to_bytes(size) do
    case Regex.run(~r/^(\d+)(mb|gb|tb)$/i, size) do
      [_, n, unit] ->
        {num, _} = Integer.parse(n)

        case String.downcase(unit) do
          "mb" -> num * 1024 * 1024
          "gb" -> num * 1024 * 1024 * 1024
          "tb" -> num * 1024 * 1024 * 1024 * 1024
        end

      _ ->
        {:error, "invalid size format: #{size}"}
    end
  end
end
