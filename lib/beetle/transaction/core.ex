defmodule Beetle.Transaction do
  @moduledoc """
  Manages transaction state and command queueing for a client session. It
  provides a clean abstraction for handling multi-step transaction.

  Beetle implements transactions in similar way to Redis transactions. To read
  more about it [here](https://redis.io/docs/latest/develop/interact/transactions/)
  """
  alias Beetle.Command

  @type t :: %__MODULE__{
          queue: term(),
          active: boolean()
        }

  defstruct queue: :queue.new(), active: false

  @doc """
  Creates a new transaction manager struct with an empty queue and inactive
  status.
  """
  def new, do: %__MODULE__{}

  @doc "Begins a new transaction if one is not already active"
  def begin(txn) when txn.active, do: {{:error, "ERR multi calls can not be nested"}, new()}
  def begin(txn), do: {"OK", %__MODULE__{txn | active: true}}

  @doc "Discards the current transaction, clearing the command queue"
  def discard(txn) when txn.active, do: {"OK", new()}
  def discard(_txn), do: {{:error, "ERR DISCARD without MULTI"}, new()}

  @doc """
  Enqueues a command to be executed when the transaction is committed.

  Commands are enqueued if a transaction is active.
  """
  def enqueue(txn, command) when txn.active,
    do: {"QUEUED", %__MODULE__{txn | queue: :queue.in(command, txn.queue)}}

  def enqueue(_, _), do: {{:error, "ERR queue command outside of multi"}, new()}

  @doc """
  Executes all queued commands in the transaction and returns the results.

  After execution, the transaction is marked as inactive and the queue is
  cleared.
  """
  def execute(txn) when txn.active do
    commands = :queue.to_list(txn.queue)
    txn = %__MODULE__{txn | active: false, queue: :queue.new()}

    Command.execute(commands, txn)
  end

  def execute(_), do: {:error, "ERR EXEC without MULTI"}
end
