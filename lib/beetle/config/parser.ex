defmodule Beetle.Config.Parser do
  @moduledoc """
  Reads & Parses configuration file for the Beetle database
  """
  require Logger
  import Beetle.Utils

  @type t :: %__MODULE__{
          port: pos_integer(),
          storage_directory: String.t(),
          database_shards: pos_integer()
        }
  defstruct(
    port: 6969,
    # 5 MB
    log_file_size: 5 * 1024,
    merge_interval: :timer.minutes(30),
    log_rotation_interval: :timer.minutes(30),
    storage_directory: Path.expand("~/.local/share/beetle"),
    database_shards: System.schedulers_online()
  )

  def read_config(nil), do: %__MODULE__{}

  def read_config(path) do
    case :file.read_file(path) do
      {:ok, content} ->
        parse_config(content)

      {:error, reason} ->
        Logger.notice("#{__MODULE__}: read_config/1 failed: #{inspect(reason)}")
        %__MODULE__{}
    end
  end

  # === Private

  @spec parse_config(String.t()) :: t()
  defp parse_config(content) do
    content
    |> String.split("\n")
    |> Enum.reject(fn val -> String.starts_with?(val, "#") or String.trim(val) == "" end)
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
  defp update_config(config, :port, value),
    do: %{config | port: String.to_integer(value)}

  defp update_config(config, :storage_directory, value) do
    path = Path.expand(value)
    updated_config = %{config | storage_directory: path}

    case :filelib.ensure_dir(path) do
      :ok -> :ok
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :database_shards, value) do
    %{config | database_shards: String.to_integer(value)}

    case parse_integer(value) do
      {:ok, shards} -> %{config | database_shards: shards}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :log_rotation_interval, value) do
    case parse_integer(value) do
      {:ok, interval_sec} -> %{config | log_rotation_interval: :timer.seconds(interval_sec)}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, :log_file_size, value) do
    case parse_integer(value) do
      {:ok, size_mb} -> %{config | log_file_size: size_mb * 1024}
      {:error, reason} -> raise reason
    end
  end

  defp merge_interval(config, :merge_interval, value) do
    case parse_integer(value) do
      {:ok, merge_interval_sec} -> %{config | merge_interval: :timer.seconds(merge_interval_sec)}
      {:error, reason} -> raise reason
    end
  end

  defp update_config(config, _, _), do: config
end
