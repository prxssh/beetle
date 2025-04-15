defmodule Beetle.Command do
  @moduledoc """
  Handles parsing and execution of Beetle commands.
  """
  alias Beetle.Protocol.{Encoder, Decoder}
  alias Beetle.Command.Mapping, as: CommandMapping

  @type t :: %__MODULE__{cmd: String.t(), args: [term()]}

  # !TODO(@prxssh): make this a struct
  @type context_t :: %{}

  defstruct [:cmd, :args]

  @doc "Parses RESP encoded command into a Beetle command struct"
  @spec parse(String.t()) :: {:ok, [t()]} | {:error, String.t()}
  def parse(resp_encoded_command) do
    dbg(resp_encoded_command)
    case Decoder.decode(resp_encoded_command) do
      {:ok, decoded} ->
        {:ok,
         Enum.map(decoded, fn [cmd | args] -> %__MODULE__{cmd: String.upcase(cmd), args: args} end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec execute(context_t(), [t()]) :: {String.t(), context_t()}
  def execute(ctx, [command]) do
    {reply, final_ctx} =
      case CommandMapping.get(command.cmd) do
        {:ok, module} -> module.handle(ctx, command.cmd, command.args)
        error -> {error, ctx}
      end

    {Encoder.encode(reply), final_ctx}
  end
end
