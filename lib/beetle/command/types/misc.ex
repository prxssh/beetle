defmodule Beetle.Command.Types.Misc do
  @moduledoc """
  Miscellaneous commands implementation
  """
  @behaviour Beetle.Command.Behaviour

  alias Beetle.Protocol.Encoder
  alias Beetle.Storage

  def handle("PING", []), do: Encoder.encode("PONG")

  def handle("PING", args) when length(args) == 1, do: args |> List.first() |> Encoder.encode()

  def handle("PING", _),
    do: Encoder.encode({:error, "ERR invalid number of arguments for command"})

  def handle("TTL", args) when length(args) != 1,
    do: Encoder.encode({:error, "ERR wrong number of arguments for command"})

  def handle("TTL", [key]) do
    key
    |> Storage.Engine.get()
    |> case do
      nil ->
        Encoder.encode(-2)

      %{expiration: 0} ->
        Encoder.encode(-1)

      %{expiration: timestamp_msec} ->
        now = System.system_time(:second)
        expiry = System.convert_time_unit(timestamp_msec, :millisecond, :second)
        ttl = expiry - now

        if ttl > 0, do: Encoder.encode(ttl), else: Encoder.encode(-2)
    end
  end
end
