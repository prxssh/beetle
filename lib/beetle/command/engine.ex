defmodule Beetle.Command.Engine do
  @moduledoc false

  alias Beetle.Command.Registry
  alias Beetle.Parser.Decoder, as: RespDecoder
  alias Beetle.Parser.Encoder, as: RespEncoder

  @type t :: %__MODULE__{
          command: String.t(),
          args: [String.t()]
        }

  defstruct [:command, :args]

  def execute(raw_command) do
    with {:ok, commands} <- parse(raw_command),
         {:ok, res} <- Registry.handle(commands) do
      {:ok, RespEncoder.encode(res)}
    else
      {:error, reason} -> RespEncoder.encode({:error, reason})
    end
  end

  @spec parse(String.t()) :: {:ok, [t()]} | {:error, String.t()}
  defp parse(raw_command) do
    raw_command
    |> RespDecoder.decode()
    |> case do
      {:ok, parsed_commands} ->
        commands =
          parsed_commands
          |> Enum.reduce([], fn parsed_command, acc ->
            command = parsed_command |> List.first()
            [_ | args] = parsed_command
            [%__MODULE__{command: String.upcase(command), args: args} | acc]
          end)

        {:ok, commands}

      error ->
        error
    end
  end
end
