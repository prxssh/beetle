defmodule Beetle.Command do
  @moduledoc false
  alias Beetle.Protocol.{
    Decoder,
    Encoder
  }

  alias Beetle.Command.Types

  @type t :: %__MODULE__{
          command: String.t(),
          args: [any()]
        }
  defstruct [:command, :args]

  @string_commands ~w(GET SET DEL APPEND GETDEL GETEX GETRANGE GETSET STRLEN SUBSTR)
  @misc_commands ~w(PING TTL)

  @spec parse(String.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(resp_encoded_command) do
    resp_encoded_command
    |> Decoder.decode()
    |> case do
      {:ok, decoded} ->
        {:ok,
         %__MODULE__{
           command: decoded |> List.first() |> String.upcase(),
           args: List.delete_at(decoded, 0)
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec execute(t()) :: String.t()
  def execute(%{command: command, args: args}) do
    command
    |> get_command_module()
    |> case do
      {:ok, module} -> module.handle(command, args)
      error -> Encoder.encode(error)
    end
  end

  # ==== Private

  defp get_command_module(command) do
    cond do
      command in @string_commands -> {:ok, Types.String}
      command in @misc_commands -> {:ok, Types.Misc}
      true -> {:error, "ERR unknown command '#{command}'"}
    end
  end
end

defmodule Beetle.Command.Behaviour do
  @moduledoc false
  @callback handle(command :: String.t(), args :: [any()]) :: String.t()
end
