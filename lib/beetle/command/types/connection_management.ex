defmodule Beetle.Command.Types.ConnectionManagment do
  @moduledoc """
  Commands that are utlizied for [connection management](https://redis.io/docs/latest/commands/?group=connection).
  """
  @behaviour Beetle.Command.Behaviour

  ########## PING

  def handle(ctx, "PING", []), do: {"PONG", ctx}
  def handle(ctx, "PING", args) when length(args) == 1, do: {List.first(args), ctx}
  def handle(ctx, "PING", _), do: {{:error, "ERR invalid number of arguments for command"}, ctx}
end
