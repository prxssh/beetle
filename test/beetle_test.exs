defmodule BeetleTest do
  use ExUnit.Case
  doctest Beetle

  test "greets the world" do
    assert Beetle.hello() == :world
  end
end
