defmodule Beetle.Command.Types.ConnectionManagment do
  @moduledoc """
  Commands that are utlizied for [connection management](https://redis.io/docs/latest/commands/?group=connection).
  """
  import Beetle.Utils

  @behaviour Beetle.Command.Behaviour

  ########## PING

  def handle(ctx, "PING", []), do: {"PONG", ctx}
  def handle(ctx, "PING", args) when length(args) == 1, do: {List.first(args), ctx}
  def handle(ctx, "PING", _), do: {error_command_arguments("PING"), ctx}

  ########## ECHO

  def handle(ctx, "ECHO", [arg]), do: {arg, ctx}
  def handle(ctx, "ECHO", _), do: {error_command_arguments("ECHO"), ctx}
end
