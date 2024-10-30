defmodule Beetle.Config do
  @moduledoc """
  Configuration parser and manager for BeetleDB.
  """

  @type t :: %__MODULE__{
          port: integer(),
          bind: String.t(),
          save_path: String.t(),
          snapshot_frequency: integer(),
          append_only: boolean()
        }

  defstruct port: 6969,
            bind: "127.0.0.1",
            save_path: "data",
            snapshot_frequency: 3600,
            append_only: true

  @doc """
  Loads configuration for a file path.

  If file is not found or is invalid, returns default configuration.
  """
  @spec load(String.t()) :: {:ok, t()} | {:error, String.t()}
  def load(path \\ "data/beetle.conf") do
    path
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
      :error -> config
    end
  end

  defp apply_config("bind", value, config), do: %{config | bind: value}
  defp apply_config("save_path", value, config), do: %{config | save_path: value}
  defp apply_config("max_clients", value, config), do: %{config | max_clients: value}

  defp apply_config("snapshot_frequency", value, config) do
    value
    |> Integer.parse()
    |> case do
      {snapshot_frequency, _} -> %{config | snapshot_frequency: snapshot_frequency}
      :error -> config
    end
  end

  defp apply_config("append_only", value, config) do
    value =
      value
      |> String.downcase()
      |> case do
        "true" -> true
        "false" -> false
        _ -> false
      end

    %{config | append_only: value}
  end
end
