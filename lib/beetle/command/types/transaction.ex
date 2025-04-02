defmodule Beetle.Command.Types.Transaction do
  @moduledoc """
  Commands for Transactions
  """
  @behaviour Beetle.Command.Behaviour

  alias Beetle.Transaction

  def with_context(transaction_context, "MULTI", _args),
    do: Transaction.begin(transaction_context)

  def with_context(transaction_context, "EXEC", _args),
    do: Transaction.execute(transaction_context)

  def with_context(transaction_context, "DISCARD", _args),
    do: Transaction.discard(transaction_context)
end
