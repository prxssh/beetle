defmodule Beetle.Command.Types.String do
  @moduledoc """
  String data type commands implementation
  """
  @behaviour Beetle.Command.Behaviour

  alias Beetle.Utils
  alias Beetle.Storage
  alias Beetle.Protocol.Encoder

  @typep set_opts_t :: %{
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

  def handle("GET", args) when length(args) != 1, do: error_command_arguments("GET")

  def handle("GET", args) do
    args
    |> List.first()
    |> Storage.Engine.get()
    |> case do
      nil -> nil
      %{value: value} -> value
    end
  end

  def handle("SET", args) when length(args) < 2 or length(args) > 5,
    do: error_command_arguments("SET")

  def handle("SET", args) do
    with {key, value} <- {Enum.at(args, 0), Enum.at(args, 1)},
         {:ok, options} <- args |> Enum.slice(2..-1//1) |> parse_set_options() do
      execute_set(key, value, options)
    else
      error -> error
    end
  end

  def handle("DEL", args) when length(args) < 1, do: error_command_arguments("DEL")

  def handle("DEL", args), do: Storage.Engine.drop(args)

  def handle("APPEND", args) when length(args) != 2, do: error_command_arguments("APPEND")

  def handle("APPEND", args) do
    {key, value} = {List.first(args), List.last(args)}

    key
    |> Storage.Engine.get()
    |> case do
      nil ->
        Storage.Engine.put(key, value, 0)
        byte_size(value)

      %{value: older_value} ->
        new_value = older_value <> value
        Storage.Engine.put(key, new_value, 0)
        byte_size(new_value)
    end
  end

  def handle("GETDEL", args) when length(args) != 1, do: error_command_arguments("GETDEL")

  def handle("GETDEL", args) do
    key = List.first(args)
    value = Storage.Engine.get(key)

    if is_nil(value), do: nil, else: Storage.Engine.drop(key)

    value
  end

  def handle("GETEX", args) when length(args) < 2 or length(args) > 3,
    do: error_command_arguments("GETEX")

  def handle("GETEX", args) do
    {key, opts} = List.pop_at(0, [])
    value = Storage.Engine.get_value(key)

    case parse_getex_options(opts) do
      {:ok, {nil, _}} ->
        value

      {:ok, {expiration, _}} ->
        Storage.Engine.put(key, value, expiration)
        value
    end
  end

  def handle("GETRANGE", args) when length(args) != 3, do: error_command_arguments("GETRANGE")

  def handle("GETRANGE", args) do
    [key, start, stop] = args

    with {:ok, start} <- Utils.parse_integer(start),
         {:ok, stop} <- Utils.parse_integer(stop) do
      key
      |> Storage.Engine.get()
      |> case do
        nil ->
          ""

        %{value: value} when is_binary(value) ->
          value |> slice_string(start, stop)

        _ ->
          {:error, "operation against a  key holding the wrong kind of value"}
      end
    else
      error -> error
    end
  end

  def handle("STRLEN", args) when length(args) != 1, do: error_command_arguments("STRLEN")

  def handle("STRLEN", [key]) do
    key
    |> Storage.Engine.get_value()
    |> case do
      nil -> 0
      value -> String.length(value)
    end
  end

  def handle("DECR", args) when length(args) != 1, do: error_command_arguments("DECR")

  def handle("DECR", [key | _]), do: handle("DECRBY", [key, 1])

  def handle("DECRYBY", args) when length(args) != 2, do: error_command_arguments("DECRBY")

  def handle("DECRYBY", args) do
    {key, value} = {List.first(args), List.last(args)}

    with {:ok, decr} <- to_integer(value),
         current_value <- Storage.Engine.get(key),
         decremented_value <- calculate_new_value(current_value, decr),
         :ok <- Storage.Engine.put(key, value, decremented_value) do
      decremented_value
    else
      {:error, _} -> {:error, "value is not integer or out of range"}
    end
  end

  def handle("INCR", args) do
  end

  def handle("INCRBY", args) do
  end

  # ==== Private

  @spec error_command_arguments(String.t()) :: {:error, String.t()}
  defp error_command_arguments(command),
    do: {:error, "ERR invalid number of arguments for '#{command}' command"}

  defp error_syntax, do: {:error, "syntax error"}

  @spec parse_set_options([String.t()], set_opts_t()) ::
          {:ok, set_opts_t()} | {:error, String.t()}
  defp parse_set_options(opts, acc \\ @default_set_opts)

  defp parse_set_options([], acc), do: {:ok, acc}

  defp parse_set_options([opt | rest], acc) do
    opt
    |> String.upcase()
    |> do_parse_option(rest, acc)
  end

  @spec do_parse_option(String.t(), [String.t()], set_opts_t()) ::
          set_opts_t() | {:error, String.t()}
  defp do_parse_option("NX", _, %{set_when_exists: true}), do: {:error, "syntax error"}

  defp do_parse_option("NX", rest, acc),
    do: parse_set_options(rest, Map.put(acc, :set_when_not_exists, true))

  defp do_parse_option("XX", _, %{set_when_not_exists: true}), do: {:error, "syntax error"}

  defp do_parse_option("XX", rest, acc),
    do: parse_set_options(rest, Map.put(acc, :set_when_exists, true))

  defp do_parse_option("GET", rest, acc), do: parse_set_options(rest, Map.put(acc, :get, true))

  defp do_parse_option("KEEPTTL", _, acc) when acc.expires_at > 0, do: {:error, "syntax error"}

  defp do_parse_option("KEEPTTL", rest, acc),
    do: parse_set_options(rest, Map.put(acc, :keepttl, true))

  defp do_parse_option(opt, rest, acc) when opt in @expiration_types do
    with false <- acc.expires_at > 0,
         false <- acc.keepttl,
         {:ok, {expiration, remaining}} <- parse_expiration(opt, rest) do
      parse_set_options(remaining, Map.put(acc, :expires_at, expiration))
    else
      true -> {:error, "synatx error"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_parse_option(_, _, _), do: {:error, "syntax error"}

  @spec parse_expiration(String.t(), [String.t()]) ::
          {:ok, {pos_integer(), [String.t()]}} | {:error, String.t()}
  defp parse_expiration(type, [value | rest]) do
    with {:ok, expiration} <- Utils.parse_integer(value),
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

  @spec execute_set(String.t(), term(), set_opts_t()) :: String.t()
  defp execute_set(key, value, opts) do
    current_entry = Storage.Engine.get(key)

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

  defp parse_getex_options([]), do: {:ok, {nil, []}}

  defp parse_getex_options([opt | rest]) do
    opt
    |> String.upcase()
    |> case do
      "PERSIST" -> {:ok, {0, []}}
      expiration_type -> parse_expiration(expiration_type, rest)
    end
  end

  defp slice_string(str, start, stop) do
    len = String.length(str)

    start = if start < 0, do: max(len + start, 0), else: min(start, len)
    stop = if stop < 0, do: max(len + stop, 0), else: min(stop, len)

    if start > stop, do: "", else: String.slice(str, start, stop - start + 1)
  end

  defp calculate_new_value(nil, x), do: -x
  defp calculate_new_value(val, x), do: val - x
end
