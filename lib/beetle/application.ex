defmodule Beetle.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Beetle.Worker.start_link(arg)
      # {Beetle.Worker, arg}
    ]

    opts = [strategy: :one_for_one, name: Beetle.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
