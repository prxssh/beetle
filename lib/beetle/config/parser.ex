defmodule Beetle.Config.Parser do
  @moduledoc """
  Reads & Parses configuration file for the Beetle database
  """
  require Logger

  @type t :: %__MODULE__{
          port: pos_integer(),
          storage_directory: String.t()
        }
  defstruct(
    port: 6969,
    storage_directory: "~/.local/share/beetle"
  )

  def read_config(nil), do: %__MODULE__{}

  def read_config(path) do
    case :file.read_file(path) do
      {:ok, content} -> parse_config(content)
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
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.reject(&(String.trim(&1) == ""))
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
  defp update_config(config, :port, value), do: %{config | port: String.to_integer(value)}

  defp update_config(config, :storage_directory, value) do
    path = Path.expand(value)
    updated_config = %{config | storage_directory: path}

    with false <- File.exists?(path),
         :ok <- File.mkdir_p(path) do
      updated_config
    else
      true -> updated_config
      error -> raise error
    end
  end

  defp update_config(config, _, _), do: config
end
