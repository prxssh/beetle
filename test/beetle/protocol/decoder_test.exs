defmodule Beetle.Protocol.DecoderTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Beetle.Protocol.Decoder

  describe "decode/1" do
    test "returns error for non-binary inputs" do
      assert {:error, "input must be a binary"} = Decoder.decode(123)
      assert {:error, "input must be a binary"} = Decoder.decode([1, 2, 3])
      assert {:error, "input must be a binary"} = Decoder.decode(%{})
    end

    test "decode simple string" do
      assert {:ok, "Hello"} = Decoder.decode("+Hello\r\n")
      assert {:ok, "Hello World"} = Decoder.decode("+Hello World\r\n")
    end

    test "decode simple error" do
      assert {:ok, "Error message"} = Decoder.decode("-Error message\r\n")

      assert {:ok, "WRONGTYPE Operation against a key holding the wrong kind of value"} =
               Decoder.decode(
                 "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
               )
    end

    test "decodes integer" do
      assert {:ok, 0} = Decoder.decode(":0\r\n")
      assert {:ok, 1000} = Decoder.decode(":1000\r\n")
      assert {:ok, -123} = Decoder.decode(":-123\r\n")
    end

    test "returns error for invalid integer" do
      assert {:error, "invalid integer string given for conversion"} = Decoder.decode(":abc\r\n")

      assert {:error, "invalid integer string given for conversion"} =
               Decoder.decode(":12.34\r\n")
    end

    test "decodes bulk string" do
      assert {:ok, "hello"} = Decoder.decode("$5\r\nhello\r\n")
      assert {:ok, ""} = Decoder.decode("$0\r\n\r\n")
      assert {:ok, nil} = Decoder.decode("$-1\r\n")
    end

    test "returns error for invalid bulk string" do
      assert {:error, "invalid bulk string length '-2'"} = Decoder.decode("$-2\r\n")
      assert {:error, "insufficient data for bulk string"} = Decoder.decode("$5\r\nhell\r\n")
      assert {:error, "invalid integer string given for conversion"} = Decoder.decode("$abc\r\n")
    end

    test "decodes array" do
      assert {:ok, ["hello", "world"]} = Decoder.decode("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
      assert {:ok, []} = Decoder.decode("*0\r\n")
      assert {:ok, nil} = Decoder.decode("*-1\r\n")
    end

    test "returns error for invalid array" do
      assert {:error, "invalid length of array '-2'"} = Decoder.decode("*-2\r\n")
      assert {:error, "invalid integer string given for conversion"} = Decoder.decode("*abc\r\n")
    end

    test "decodes null" do
      assert {:ok, nil} = Decoder.decode("_\r\n")
    end

    test "decodes boolean" do
      assert {:ok, true} = Decoder.decode("#t\r\n")
      assert {:ok, false} = Decoder.decode("#f\r\n")
    end

    test "returns error for invalid boolean" do
      assert {:error, "invalid type for boolean conversion"} = Decoder.decode("#x\r\n")
    end

    test "decodes double" do
      assert {:ok, 1.23} = Decoder.decode(",1.23\r\n")
      assert {:ok, -4.5} = Decoder.decode(",-4.5\r\n")
      assert {:ok, :infinity} = Decoder.decode(",inf\r\n")
      assert {:ok, :negative_infinity} = Decoder.decode(",-inf\r\n")
      assert {:ok, :nan} = Decoder.decode(",nan\r\n")
    end

    test "returns error for invalid double" do
      assert {:error, "invalid float string given for conversion"} = Decoder.decode(",abc\r\n")
    end

    test "decodes big number" do
      assert {:ok, 123_456_789_012_345_678_901_234_567_890} =
               Decoder.decode("(123456789012345678901234567890\r\n")
    end

    test "decodes bulk error" do
      assert {:ok, "Error with\r\nnewlines"} =
               Decoder.decode("!20\r\nError with\r\nnewlines\r\n")
    end

    test "decodes map" do
      input = "%2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\nname\r\n$4\r\nJohn\r\n"
      expected = %{"key" => "value", "name" => "John"}
      assert {:ok, ^expected} = Decoder.decode(input)
    end

    test "decodes empty map" do
      assert {:ok, %{}} = Decoder.decode("%0\r\n")
    end

    test "decodes set" do
      input = "~3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
      expected = MapSet.new(["a", "b", "c"])
      assert {:ok, ^expected} = Decoder.decode(input)
    end

    test "decodes empty set" do
      res = MapSet.new()
      assert {:ok, res} = Decoder.decode("~0\r\n")
    end

    test "returns error for invalid type identifier" do
      assert {:error, "invalid resp type 'X'"} = Decoder.decode("Xinvalid\r\n")
    end

    test "returns error for malformed input" do
      assert {:error, "malformed line: missing CRLF"} = Decoder.decode("+OK")
    end

    test "decodes nested structures" do
      input =
        "*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n%2\r\n$4\r\nname\r\n$4\r\nJohn\r\n$3\r\nage\r\n:30\r\n"

      expected = ["SET", "user", %{"name" => "John", "age" => 30}]
      assert {:ok, ^expected} = Decoder.decode(input)
    end
  end
end
