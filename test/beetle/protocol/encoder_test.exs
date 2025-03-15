defmodule Beetle.Protocol.EncoderTest do
  use ExUnit.Case
  alias Beetle.Protocol.Encoder

  describe "basic data types" do
    test "encodes nil to RESP Null" do
      assert Encoder.encode(nil) == "_\r\n"
    end

    test "encodes :ok to RESP Simple String" do
      assert Encoder.encode(:ok) == "+OK\r\n"
    end

    test "encodes true to RESP Boolean" do
      assert Encoder.encode(true) == "#t\r\n"
    end

    test "encodes false to RESP Boolean" do
      assert Encoder.encode(false) == "#f\r\n"
    end

    test "encodes {:error, reason} to RESP Error" do
      assert Encoder.encode({:error, "Connection failed"}) == "-Connection failed\r\n"
    end
  end

  describe "numbers" do
    test "encodes integer to RESP Integer" do
      assert Encoder.encode(42) == ":42\r\n"
      assert Encoder.encode(-10) == ":-10\r\n"
      assert Encoder.encode(0) == ":0\r\n"
    end

    test "encodes float to RESP Double" do
      assert Encoder.encode(3.14) == ",3.14\r\n"
      assert Encoder.encode(-2.5) == ",-2.5\r\n"
      assert Encoder.encode(0.0) == ",0.0\r\n"
    end
  end

  describe "strings and atoms" do
    test "encodes atom to RESP Bulk String" do
      assert Encoder.encode(:hello) == "$5\r\nhello\r\n"
      assert Encoder.encode(:elixir) == "$6\r\nelixir\r\n"
    end

    test "encodes binary to RESP Bulk String" do
      assert Encoder.encode("hello") == "$5\r\nhello\r\n"
      assert Encoder.encode("") == "$0\r\n\r\n"
      assert Encoder.encode("multi\r\nline") == "$10\r\nmulti\r\nline\r\n"
    end
  end

  describe "maps" do
    test "encodes empty map" do
      assert Encoder.encode(%{}) == "%0\r\n"
    end

    test "encodes map with single key-value pair" do
      assert Encoder.encode(%{name: "John"}) == "%1\r\n$4\r\nname\r\n$4\r\nJohn\r\n"
    end

    test "encodes map with multiple key-value pairs" do
      # Since map iteration order is not guaranteed in Elixir, we need to check
      # components
      map = %{a: 1, b: 2}
      encoded = Encoder.encode(map)

      # Check the map size indicator
      assert String.starts_with?(encoded, "%2\r\n")

      # Check for the presence of each key-value pair
      assert String.contains?(encoded, "$1\r\na\r\n:1\r\n")
      assert String.contains?(encoded, "$1\r\nb\r\n:2\r\n")
    end

    test "encodes map with nested map" do
      encoded = Encoder.encode(%{user: %{name: "John"}})

      # Check outer map size
      assert String.starts_with?(encoded, "%1\r\n")

      # Check key encoding
      assert String.contains?(encoded, "$4\r\nuser\r\n")

      # Check inner map encoding - contains size indicator and expected
      # key-value pair
      assert String.contains?(encoded, "%1\r\n$4\r\nname\r\n$4\r\nJohn\r\n")
    end
  end

  describe "lists" do
    test "encodes empty list" do
      assert Encoder.encode([]) == "*0\r\n"
    end

    test "encodes list of integers" do
      assert Encoder.encode([1, 2, 3]) == "*3\r\n:1\r\n:2\r\n:3\r\n"
    end

    test "encodes list with mixed types" do
      assert Encoder.encode([1, "hello", :world]) == "*3\r\n:1\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    end

    test "encodes nested list" do
      assert Encoder.encode([1, [2, 3]]) == "*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n"
    end
  end

  describe "complex nested structures" do
    test "encodes list containing map" do
      encoded = Encoder.encode([1, %{a: 2}])
      assert String.starts_with?(encoded, "*2\r\n:1\r\n%1\r\n")
      assert String.contains?(encoded, "$1\r\na\r\n:2\r\n")
    end

    test "encodes map containing list" do
      encoded = Encoder.encode(%{items: [1, 2]})
      assert String.starts_with?(encoded, "%1\r\n")
      assert String.contains?(encoded, "$5\r\nitems\r\n*2\r\n:1\r\n:2\r\n")
    end

    test "encodes deeply nested structure" do
      complex = %{
        user: %{
          name: "John",
          tags: ["admin", "moderator"],
          scores: %{math: 95, science: 92}
        }
      }

      encoded = Encoder.encode(complex)

      # Verify map sizes
      assert String.starts_with?(encoded, "%1\r\n")
      assert String.contains?(encoded, "$4\r\nuser\r\n%3\r\n")

      # Verify list
      assert String.contains?(encoded, "*2\r\n$5\r\nadmin\r\n$9\r\nmoderator\r\n")

      # Verify nested map values
      assert String.contains?(encoded, ":95\r\n")
      assert String.contains?(encoded, ":92\r\n")
    end
  end

  describe "unsupported data types" do
    test "raises error for unsupported data types" do
      assert_raise RuntimeError, ~r/Unsupported data format:/, fn ->
        Encoder.encode(self())
      end

      assert_raise RuntimeError, ~r/Unsupported data format:/, fn ->
        Encoder.encode(make_ref())
      end

      assert_raise RuntimeError, ~r/Unsupported data format:/, fn ->
        Encoder.encode(fn x -> x end)
      end
    end
  end
end
