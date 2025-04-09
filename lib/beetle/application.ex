defmodule Beetle.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Beetle.Config
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Beetle.Supervisor)
  end
end
