defmodule Beetle.Command do
  @moduledoc """
  Module reponsible for parsing and executing commands.
  """

  @typedoc """
  Represents a Beetle command. The fields are:
  - `command`: Uppercase command name (for e.g. GET, SET, PING, etc)
  - `args`: List of command arguments
  """
  @type t :: %__MODULE__{
          command: String.t(),
          args: [String.t()]
        }

  defstruct [:command, :args]

  @doc "Parses RESP-encoded command string into Beetle Command struct"
  @spec parse(String.t()) :: {:ok, [t()]} | {:error, String.t()}
  def parse(resp_encoded_command) do
    resp_encoded_command
    |> Beetle.Protocol.Decoder.decode()
    |> case do
      {:ok, decoded} ->
        {:ok,
         Enum.map(decoded, fn [cmd | args] ->
           %__MODULE__{
             args: args,
             command: String.upcase(cmd)
           }
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Executes a list of commands concurrently and returns combined RESP-encoded
  results.

  Commands are executed in parallel using `Task.async_stream/3` with - 2x
  available CPU schedulers and in ordere to preserve the sequence.
  """
  @spec execute([t()]) :: String.t()
  def execute(commands) do
    commands
    |> Task.async_stream(&execute_single/1,
      max_concurrency: System.schedulers_online() * 2,
      ordered: true
    )
    |> Enum.map_join("", fn {:ok, result} -> result end)
  end

  # ==== Private

  @spec execute_single(t()) :: String.t()
  defp execute_single(%__MODULE__{command: command, args: args}) do
    command
    |> Beetle.Command.Mapping.get()
    |> case do
      {:ok, module} -> module.handle(command, args)
      error -> error
    end
    |> Beetle.Protocol.Encoder.encode()
  end
end
