defmodule Beetle.Command.DataTypes.String do
  @moduledoc """
  Commands for string data type
  """
  alias Beetle.Storage.Engine, as: StorageEngine

  @spec handle(String.t(), [String.t()]) :: {:ok, any()} | {:error, String.t()}
  def handle("GET", args) when length(args) != 1, do: error_invalid_arguments("get")

  def handle("GET", args) do
    args
    |> List.first()
    |> StorageEngine.get()
    |> case do
      {:ok, res} -> {:ok, res}
      _ -> {:ok, nil}
    end
  end

  def handle("SET", args) when length(args) < 2 or length(args) > 5,
    do: error_invalid_arguments("set")

  def handle("SET", _args) do
    {:ok, nil}
  end

  defp error_invalid_arguments(command),
    do: {:error, "ERR wrong number of arguments for #{command} command"}
end
