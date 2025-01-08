defmodule Beetle.Config.Parser do
  @moduledoc """
  Reads & Parses configuration file for the Beetle database
  """

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
      {:error, _} -> %__MODULE__{}
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

  defp update_config(config, :storage_directory, value),
    do: %{config | storage_directory: Path.expand(value)}

  defp update_config(config, _, _), do: config
end
