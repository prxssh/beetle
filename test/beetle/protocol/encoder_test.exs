defmodule Beetle.Protocol.EncoderTest do
  use ExUnit.Case, async: true
  alias Beetle.Protocol.Encoder

  describe "encode/1" do
    test "encodes nil" do
      assert Encoder.encode(nil) == "_\r\n"
    end

    test "encodes :ok" do
      assert Encoder.encode(:ok) == "+OK\r\n"
    end

    test "encodes boolean true/false" do
      assert Encoder.encode(true) == "#t\r\n"
      assert Encoder.encode(false) == "#f\r\n"
    end

    test "encodes {:error, reason}" do
      assert Encoder.encode({:error, "Something went wrong"}) ==
               "-$20\r\nSomething went wrong\r\n\r\n"
    end

    test "encodes float" do
      assert Encoder.encode(1.23) == ",1.23\r\n"
    end

    test "encodes integer" do
      assert Encoder.encode(42) == ":42\r\n"
    end

    test "encodes atom (other than :ok)" do
      assert Encoder.encode(:foo) == "$3\r\nfoo\r\n"
    end

    test "encodes binary" do
      assert Encoder.encode("bar") == "$3\r\nbar\r\n"
    end

    test "encodes list" do
      assert Encoder.encode(["foo", 42, true]) ==
               "*3\r\n$3\r\nfoo\r\n:42\r\n#t\r\n"
    end

    test "encodes map (order-agnostic)" do
      map = %{"foo" => 1, "bar" => true}
      encoded = Encoder.encode(map)

      assert encoded =~ "%2\r\n"
      assert encoded =~ "$3\r\nfoo\r\n:1\r\n"
      assert encoded =~ "$3\r\nbar\r\n#t\r\n"
    end

    test "raises on unsupported type (e.g. tuple not matching {:error, reason})" do
      assert_raise RuntimeError, ~r/Unsupported data format/, fn ->
        Encoder.encode({:ok, :extra})
      end
    end
  end
end
