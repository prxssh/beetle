defmodule Beetle.MixProject do
  use Mix.Project

  def project do
    [
      app: :beetle,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :tools, :runtime_tools],
      mod: {Beetle.Application, []}
    ]
  end

  defp deps do
    [
      {:mock, "~> 0.3.0", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end
end
