defmodule Beetle.Command.Types.String do
  @moduledoc """
  Commands for [strings data type](https://redis.io/docs/latest/commands/?group=string)
  """
  alias Beetle.Storage.Engine, as: StorageEngine

  ########## GET

  # GET key
  def handle(ctx, "GET", [key]) do
    res =
      key
      |> StorageEngine.get_value()
      |> case do
        {:ok, value} -> value
        error -> error
      end

    {res, ctx}
  end

  def handle(ctx, "GET", _), do: {error_command_arguments("GET"), ctx}

  ########## SET

  @type set_opts_t :: %{
          get: boolean(),
          keepttl: boolean(),
          set_when_exists: boolean(),
          expires_at: non_neg_integer(),
          set_when_not_exists: boolean()
        }

  @default_set_opts %{
    get: false,
    expires_at: 0,
    keepttl: false,
    set_when_exists: false,
    set_when_not_exists: false
  }

  @expiration_types ~w(EX PX EXAT PXAT)

  # SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
  #  EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
  def handle(ctx, "SET", args) when length(args) < 2 or length(args) > 5,
    do: {error_command_arguments("SET"), ctx}

  def handle(ctx, "SET", args) do
    with {key, value} <- {Enum.at(args, 0), Enum.at(args, 1)},
         {:ok, set_opts} <- args |> Enum.slice(2..-1//1) |> parse_set_opts() do
      execute_set(ctx, key, value, set_opts)
    else
      error -> {error, ctx}
    end
  end

  @spec parse_set_opts([String.t()], set_opts_t()) :: {:ok, set_opts_t()} | {:error, String.t()}
  defp parse_set_opts(opts, acc \\ @default_set_opts)

  defp parse_set_opts([], acc), do: {:ok, acc}

  defp parse_set_opts([opt | rest], acc) do
    opt
    |> String.upcase()
    |> do_parse_set_opts(rest, acc)
  end

  @spec do_parse_set_opts(String.t(), [String.t()], set_opts_t()) ::
          set_opts_t() | {:error, String.t()}
  defp do_parse_set_opts("NX", _, %{set_when_exists: true}), do: {:error, "syntax error"}

  defp do_parse_set_opts("NX", rest, acc),
    do: parse_set_opts(rest, Map.put(acc, :set_when_not_exists, true))

  defp do_parse_set_opts("XX", _, %{set_when_not_exists: true}), do: {:error, "syntax error"}

  defp do_parse_set_opts("XX", rest, acc),
    do: parse_set_opts(rest, Map.put(acc, :set_when_exists, true))

  defp do_parse_set_opts("GET", rest, acc), do: parse_set_opts(rest, Map.put(acc, :get, true))

  defp do_parse_set_opts("KEEPTTL", _, acc) when acc.expires_at > 0, do: {:error, "syntax error"}

  defp do_parse_set_opts("KEEPTTL", rest, acc),
    do: parse_set_opts(rest, Map.put(acc, :keepttl, true))

  defp do_parse_set_opts(opt, rest, acc) when opt in @expiration_types do
    with false <- acc.expires_at > 0,
         false <- acc.keepttl,
         {:ok, {expiration, remaining}} <- parse_expiration(opt, rest) do
      parse_set_opts(remaining, Map.put(acc, :expires_at, expiration))
    else
      true -> {:error, "synatx error"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_parse_set_opts(_, _, _), do: {:error, "syntax error"}

  @spec parse_expiration(String.t(), [String.t()]) ::
          {:ok, {pos_integer(), [String.t()]}} | {:error, String.t()}
  defp parse_expiration(type, [value | rest]) do
    with {:ok, expiration} <- Beetle.Utils.parse_integer(value),
         true <- expiration > 0,
         {:ok, timestamp} <- compute_epxiration(type, expiration) do
      {:ok, {timestamp, rest}}
    else
      false -> {:error, "invalid expire time in command"}
      error -> error
    end
  end

  defp compute_expiration("PXAT", timestamp), do: {:ok, timestamp}
  defp compute_expiration("EXAT", timestamp), do: {:ok, timestamp * 1000}

  defp compute_epxiration("EX", seconds),
    do: {:ok, System.system_time(:millisecond) + seconds * 1000}

  defp compute_epxiration("PX", milliseconds),
    do: {:ok, System.system_time(:millisecond) + milliseconds}

  defp compute_expiration(_, _), do: :error

  @spec execute_set(Beetle.Command.context_t(), String.t(), String.t(), set_opts_t()) :: {String.t(), Beetle.Command.context_t()}
  defp execute_set(ctx, key, value, opts) do
    current_entry = StorageEngine.get(key)

    return_value =
      cond do
        opts.set_when_exists and is_nil(current_entry) ->
          nil

        opts.set_when_not_exists and not is_nil(current_entry) ->
          nil

        # already expired
        opts.keepttl and is_nil(current_entry) ->
          :ok

        # persist current ttl
        opts.keepttl and not is_nil(current_entry) ->
          Storage.Engine.put(key, value, current_entry.expiration)

        true ->
          Storage.Engine.put(key, value, opts.expires_at)
      end

    if opts.get,
      do: Map.get(current_entry, :value),
      else: return_value
  end

  ########## DEL

  ########## APPEND

  ########## GETDEL

  ########## GETEX

  ########## GETRANGE

  ########## STRLEN

  ########## DECR

  ########## DECRBY

  ########## INCR

  ########## INCRBY

  ############### Private

  @spec error_command_arguments(String.t()) :: {:error, String.t()}

  defp error_command_arguments(command),
    do: {:error, "ERR invalid number of arguments for '#{command}' command"}
end
