defmodule Beetle.Utils do
  @moduledoc """
  Common utility functions
  """

  def to_boolean("true"), do: true
  def to_boolean(_), do: false
end
