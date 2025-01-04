defmodule Beetle.Protocol.EncoderTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Beetle.Protocol.Encoder

  describe "encode/1 for nulls" do
    test "encode nil" do
      assert Encoder.encode(nil) == "_\r\n"
    end
  end

  describe "encode/1 for boolean" do
    test "encode boolean true" do
      assert Encoder.encode(true) == "#t\r\n"
    end

    test "encode boolean false" do
      assert Encoder.encode(false) == "#f\r\n"
    end
  end

  describe "encode/1 for errors" do
    test "encode normal error" do
      assert Encoder.encode({:error, "syntax error"}) == "-syntax error\r\n"
    end
  end

  describe "encode/1 for strings and atoms" do
    test "encode atom" do
      assert Encoder.encode(:hello) == "$5\r\nhello\r\n"
    end

    test "encode string" do
      assert Encoder.encode("hello world") == "$11\r\nhello world\r\n"
    end
  end

  describe "encode/1 for numbers" do
    test "encode positive integer" do
      assert Encoder.encode(500_000) == ":500000\r\n"
    end

    test "encode negative integer" do
      assert Encoder.encode(-500_000) == ":-500000\r\n"
    end

    test "encode positive float" do
      assert Encoder.encode(500_000.2345) == ",500000.2345\r\n"
    end

    test "encode negative float integer" do
      assert Encoder.encode(-500_000.2345) == ",-500000.2345\r\n"
    end

    test "encode really big number" do
      assert Encoder.encode(4_943_957_349_753_957_395_739_579_375_935_793) ==
               ":4943957349753957395739579375935793\r\n"
    end
  end

  describe "encode/1 for list" do
    test "encode empty list" do
      assert Encoder.encode([]) == "*0\r\n"
    end

    test "encode string list" do
      assert Encoder.encode(["hello", "world"]) == "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    end

    test "encode mixed type list" do
      mixed_list = [
        "hello",
        "world",
        100,
        -100,
        200.512,
        -300.534,
        593_457_353_453_535,
        [5, 6, 7, 8, "another", "list"]
      ]

      assert Encoder.encode(mixed_list) ==
               "*8\r\n$5\r\nhello\r\n$5\r\nworld\r\n:100\r\n:-100\r\n,200.512\r\n,-300.534\r\n:593457353453535\r\n*6\r\n:5\r\n:6\r\n:7\r\n:8\r\n$7\r\nanother\r\n$4\r\nlist\r\n"
    end
  end

  describe "encode/1 for map" do
    test "encode empty map" do
      assert Encoder.encode(%{}) == "%0\r\n"
    end

    test "encode string map" do
      assert Encoder.encode(%{hello: "world", elixir: "map"}) ==
               "%2\r\n$6\r\nelixir\r\n$3\r\nmap\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    end

    test "encode mixed types map" do
      mixed_map = %{
        hello: "world",
        age: 30,
        salary: 2000.523,
        phone: [1, 2, 3, 4, 5]
      }

      assert Encoder.encode(mixed_map) ==
               "%4\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nage\r\n:30\r\n$6\r\nsalary\r\n,2000.523\r\n$5\r\nphone\r\n*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n:5\r\n"
    end
  end
end
