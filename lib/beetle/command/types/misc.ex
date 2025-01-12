defmodule Beetle.Command.Types.Misc do
  @moduledoc """
  Miscellaneous commands implementation
  """
  @behaviour Beetle.Command.Behaviour

  alias Beetle.Protocol.Encoder

  def handle("PING", args) when length(args) == 0, do: Encoder.encode("PONG")

  def handle("PING", args) when length(args) == 1, do: args |> List.first() |> Encoder.encode()

  def handle("PING", args),
    do: Encoder.encode({:error, "ERR invalid number of arguments for 'PING' command"})
end
