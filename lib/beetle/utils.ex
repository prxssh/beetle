defmodule Beetle.Utils do
  @moduledoc false

  @spec serialize(term()) :: binary()
  def serialize(term), do: :erlang.term_to_binary(term, [:deterministic, :compressed])

  @spec deserialize(binary()) :: {:ok, term()} | {:error, :malformed_binary_type}
  def deserialize(binary) when is_binary(binary), do: {:ok, :erlang.binary_to_term(binary)}
  def deserialize(_), do: {:error, :malformed_binary_type}

  def parse_integer(str) do
    case Integer.parse(str) do
      {num, ""} -> {:ok, num}
      _ -> {:error, "value is not an integer or out of range"}
    end
  end

  @doc """
  Creates the directory at `path` if it doesn't already exists.
  """
  @spec maybe_create_directory(Path.t()) :: :ok | {:error, any()}
  def maybe_create_directory(path) do
    with path <- to_charlist(path),
         {:ok, _} <- :file.read_file_info(path) do
      :ok
    else
      {:error, :enoent} -> :file.make_dir(path)
      {:error, reason} -> {:error, reason}
    end
  end
end
