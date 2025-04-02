defmodule Beetle.Command do
  @moduledoc """
  Module reponsible for parsing and executing commands.
  """

  @typedoc """
  Represents a Beetle command. The fields are:
  - `command`: Uppercase command name (for e.g. GET, SET, PING, etc)
  - `args`: List of command arguments
  """
  alias Beetle.Command.Types.Transaction
  alias JasonV.Encoder
  alias Beetle.Transaction
  alias Beetle.Command.Mapping
  alias Beetle.Protocol.{Decoder, Encoder}

  @type t :: %__MODULE__{
          cmd: String.t(),
          args: [String.t()]
        }

  defstruct [:cmd, :args]

  @doc "Parses RESP-encoded command string into Beetle Command struct"
  @spec parse(String.t()) :: {:ok, [t()]} | {:error, :command_parse, String.t()}
  def parse(resp_encoded_command) do
    resp_encoded_command
    |> Decoder.decode()
    |> case do
      {:ok, decoded} ->
        {:ok,
         Enum.map(decoded, fn [cmd | args] ->
           %__MODULE__{
             args: args,
             cmd: String.upcase(cmd)
           }
         end)}

      {:error, reason} ->
        {:error, :command_parse, Encoder.encode({:error, reason})}
    end
  end

  @doc "Executes commands concurrently within a transaction context."
  @spec execute([t()], Transaction.t()) :: {String.t(), Transaction.t()}
  def execute(commands, transaction_context) do
    {result, updated_transaction_context} =
      commands
      |> Task.async_stream(
        fn %__MODULE__{cmd: cmd, args: args} ->
          case Mapping.get(cmd) do
            {:with_context, module} -> module.with_context(transaction_context, cmd, args)
            {:handle, module} -> {module.handle(cmd, args), nil}
            error -> {error, nil}
          end
        end,
        ordered: true,
        max_concurrency: System.schedulers_online() * 2
      )
      |> Enum.reduce({[], transaction_context}, fn
        {:ok, {res, nil}}, {acc_result, txn_context} -> {[res | acc_result], txn_context}
        {:ok, {res, txn_context}}, {acc_result, _} -> {[res | acc_result], txn_context}
        stream_error, {acc_result, txn_context} -> {[stream_error | acc_result], txn_context}
      end)
      |> then(fn {res, txn} -> {Enum.reverse(res), txn} end)

    response =
      if transaction_context.active do
        Enum.map_join(result, "", &Encoder.encode/1)
      else
        case result do
          [res] -> Encoder.encode(res)
          _ -> Encoder.encode(result)
        end
      end

    {response, updated_transaction_context}
  end
end
