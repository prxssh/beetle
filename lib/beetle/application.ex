defmodule Beetle.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    path = maybe_extract_path(System.argv())

    children = [
      {Beetle.Server, []},
      {Beetle.Config, path},
      {Beetle.Storage.Supervisor, []},
      {Beetle.Server.ClientSupervisor, []}
    ]

    opts = [strategy: :one_for_one, name: Beetle.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp maybe_extract_path([]), do: nil
  defp maybe_extract_path([_path, location]), do: Path.expand(location)
end
