defmodule Beetle.Command do
  @moduledoc """
  Module reponsible for parsing and executing commands.
  """

  @typedoc """
  Represents a Beetle command. The fields are:
  - `command`: Uppercase command name (for e.g. GET, SET, PING, etc)
  - `args`: List of command arguments
  """
  alias Beetle.Protocol.Encoder

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
  @spec execute([t()], keyword()) :: String.t()
  def execute(commands, opts \\ [])

  def execute(commands, opts) when is_list(commands) do
    results_stream =
      commands
      |> Task.async_stream(&execute/1,
        max_concurrency: System.schedulers_online() * 2,
        ordered: true
      )
      |> Stream.map(fn {:ok, result} -> result end)

    if Keyword.get(opts, :transaction, false),
      do: results_stream |> Enum.to_list() |> Encoder.encode(),
      else: Enum.map_join(results_stream, "", &Encoder.encode/1)
  end

  def execute(%__MODULE__{command: command, args: args}, _) do
    command
    |> Beetle.Command.Mapping.get()
    |> case do
      {:ok, module} -> module.handle(command, args)
      error -> error
    end
  end

  def execute_transaction(commands), do: execute(commands, transaction: true)
end
