defmodule Beetle.Server.TransactionManager do
  @moduledoc """
  Manages transaction state and command queueing for a client session. It
  provides a clean abstraction for handling multi-step transaction.

  Beetle implements transactions in similar way to Redis transactions. To read
  more about it: https://redis.io/docs/latest/develop/interact/transactions/
  """
  alias Beetle.{
    Command,
    Protocol.Encoder
  }

  defstruct queue: :queue.new(), active: false

  def new, do: %__MODULE__{}

  def begin(manager) do
    if manager.active,
      do: {:error, "ERR multi calls can not be nested"},
      else: {:ok, {Encoder.encode("OK"), %{manager | active: true}}}
  end

  def discard(manager) do
    if manager.active,
      do: {:ok, {Encoder.encode("OK"), new()}},
      else: {:error, "ERR DISCARD without MULTI"}
  end

  def enqueue(manager, command) do
    if manager.active do
      {:ok, {Encoder.encode("QUEUED"), %{manager | queue: :queue.in(command, manager.queue)}}}
    else
      {:error, "ERR queue command outside of multi"}
    end
  end

  def execute(manager) do
    if manager.active do
      commands = :queue.to_list(manager.queue)
      updated_state = %{manager | active: false, queue: :queue.new()}

      {:ok, {Command.execute_transaction(commands), updated_state}}
    else
      {:error, "ERR EXEC without MULTI"}
    end
  end
end
