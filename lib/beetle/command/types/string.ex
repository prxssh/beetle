defmodule Beetle.Command.Types.String do
  @moduledoc """
  String data type commands implementation
  """
  @behaviour Beetle.Command.Behaviour

  alias Beetle.Storage
  alias Beetle.Protocol.Encoder

  # GET key
  def handle("GET", args) when length(args) != 1, do: error_command_arguments("GET")

  def handle("GET", args) do
    args
    |> List.first()
    |> Storage.Engine.get()
    |> Encoder.encode()
  end

  # SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
  def handle("SET", args) when length(args) < 2 or length(args) > 5,
    do: error_command_arguments("SET")

  def handle("SET", args) do
    {key, value} = {Enum.at(args, 0), Enum.at(args, 1)}
    # opts = args |> Enum.slice(2..-1) |> parse_set_options()

    key
    |> Storage.Engine.put(value, 0)
    |> case do
      :ok -> Encoder.encode("OK")
      error -> error_unable_to_execute("SET")
    end
  end

  def handle("DEL", args) when length(args) < 1, do: error_syntax()

  def handle("DEL", args) do
    args
    |> Storage.Engine.drop()
    |> Encoder.encode()
  end

  # ==== Private

  @typep set_opts_t :: %{
           get: boolean(),
           keepttl: boolean(),
           set_when_exists: boolean(),
           expires_at: non_neg_integer(),
           set_when_not_exists: boolean()
         }

  @spec error_command_arguments(String.t()) :: String.t()
  defp error_command_arguments(command) do
    reason = "ERR invalid number of arguments for '#{command}' command"
    Encoder.encode({:error, reason})
  end

  @spec error_unable_to_execute(String.t()) :: String.t()
  defp error_unable_to_execute(command),
    do: Encoder.encode({:error, "something went wrong when executing '#{command}' command"})

  defp error_syntax, do: Encoder.encode({:error, "syntax error"})

  defp parse_set_options(opts) do
  end
end
