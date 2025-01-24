defmodule Beetle.Command.Types.Misc do
  @moduledoc """
  Miscellaneous commands implementation
  """
  @behaviour Beetle.Command.Behaviour

  alias Beetle.Storage

  def handle("PING", []), do: "PONG"

  def handle("PING", args) when length(args) == 1, do: args |> List.first() |> Encoder.encode()

  def handle("PING", _), do: {:error, "ERR invalid number of arguments for command"}

  def handle("TTL", args) when length(args) != 1,
    do: {:error, "ERR wrong number of arguments for command"}

  def handle("TTL", [key]) do
    key
    |> Storage.Engine.get()
    |> case do
      nil ->
        -2

      %{expiration: 0} ->
        -1

      %{expiration: timestamp_msec} ->
        now = System.system_time(:second)
        expiry = System.convert_time_unit(timestamp_msec, :millisecond, :second)
        ttl = expiry - now

        if ttl > 0, do: ttl, else: -2
    end
  end
end
