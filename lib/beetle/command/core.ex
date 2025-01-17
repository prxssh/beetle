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

  @misc_commands ~w(PING TTL)
  @bitmap_commands ~w(BITCOUNT, BITFIELD, BITFIELD_RO, BITOP, BITPOS, GETBIT, SETBIT)
  @string_commands ~w(GET SET DEL APPEND GETDEL GETEX GETRANGE STRLEN DECR DECRBY INCR INCRBY)
  @list_commands ~w(LINDEX LINSERT LLEN LMOVE LMPOP LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM RPOP RPOPLPUSH RPUSH RPUSHX)
  @hash_commands ~w(HDEL HEXISTS HEXPIRE HEXPIREAT HEXPIRETIME HGET HGETALL HINCRBY HKEYS HLEN HMMGET HMSET HPERSIST HPEXPIRE HPEXPIREAT HPEXPIRETIME, HPTTL, HRANDIFIELD, HSCAN, HSET, HSTRLEN, HTTL, HVALS)

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
      command in @misc_commands -> {:ok, Types.Misc}
      command in @string_commands -> {:ok, Types.String}
      command in @list_commands -> {:ok, Types.List}
      command in @hash_commands -> {:ok, Types.Hash}
      command in @bitmap_commands -> {:ok, Types.Bitmap}
      true -> {:error, "ERR unknown command '#{command}'"}
    end
  end
end

defmodule Beetle.Command.Behaviour do
  @moduledoc false
  @callback handle(command :: String.t(), args :: [any()]) :: String.t()
end
