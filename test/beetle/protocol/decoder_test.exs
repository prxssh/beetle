defmodule Beetle.Protocol.DecoderTest do
  use ExUnit.Case, async: true
  alias Beetle.Protocol.Decoder

  describe "basic decoding" do
    test "decodes empty input" do
      assert {:ok, []} = Decoder.decode("")
      assert {:ok, []} = Decoder.decode(<<>>)
    end

    test "rejects non-binary input" do
      assert {:error, "input must be a binary"} = Decoder.decode(123)
      assert {:error, "input must be a binary"} = Decoder.decode(:atom)
      assert {:error, "input must be a binary"} = Decoder.decode(['list'])
      assert {:error, "input must be a binary"} = Decoder.decode(%{})
    end
  end

  describe "simple string decoding" do
    test "decodes simple string" do
      assert {:ok, ["OK"]} = Decoder.decode("+OK\r\n")
      assert {:ok, ["Hello"]} = Decoder.decode("+Hello\r\n")
      assert {:ok, [""]} = Decoder.decode("+\r\n")
    end

    test "decodes multiple simple strings" do
      assert {:ok, ["First", "Second"]} = Decoder.decode("+First\r\n+Second\r\n")
    end

    test "handles malformed simple string" do
      assert {:error, "malformed line: missing CRLF"} = Decoder.decode("+OK")
    end
  end

  describe "simple error decoding" do
    test "decodes simple error" do
      assert {:ok, ["Error"]} = Decoder.decode("-Error\r\n")
      assert {:ok, ["ERR unknown command"]} = Decoder.decode("-ERR unknown command\r\n")
    end
  end

  describe "integer decoding" do
    test "decodes positive integer" do
      assert {:ok, [10]} = Decoder.decode(":10\r\n")
      assert {:ok, [1000]} = Decoder.decode(":1000\r\n")
    end

    test "decodes negative integer" do
      assert {:ok, [-5]} = Decoder.decode(":-5\r\n")
      assert {:ok, [-1000]} = Decoder.decode(":-1000\r\n")
    end

    test "decodes zero" do
      assert {:ok, [0]} = Decoder.decode(":0\r\n")
    end

    test "handles invalid integer format" do
      assert {:error, _} = Decoder.decode(":not_an_integer\r\n")
    end
  end

  describe "bulk string decoding" do
    test "decodes bulk string" do
      assert {:ok, ["hello"]} = Decoder.decode("$5\r\nhello\r\n")
      assert {:ok, ["world"]} = Decoder.decode("$5\r\nworld\r\n")
    end

    test "decodes empty bulk string" do
      assert {:ok, [""]} = Decoder.decode("$0\r\n\r\n")
    end

    test "decodes null bulk string" do
      assert {:ok, [nil]} = Decoder.decode("$-1\r\n")
    end

    test "handles invalid bulk string length" do
      assert {:error, "invalid bulk string length '-2'"} = Decoder.decode("$-2\r\n")
    end

    test "handles insufficient data for bulk string" do
      assert {:error, "insufficient data for bulk string"} =
               Decoder.decode("$10\r\ninsuffici\r\n")
    end
  end

  describe "array decoding" do
    test "decodes empty array" do
      assert {:ok, [[]]} = Decoder.decode("*0\r\n")
    end

    test "decodes null array" do
      assert {:ok, [nil]} = Decoder.decode("*-1\r\n")
    end

    test "handles invalid array length" do
      assert {:error, "invalid length of array '-2'"} = Decoder.decode("*-2\r\n")
    end

    test "decodes array of integers" do
      assert {:ok, [[1, 2, 3]]} = Decoder.decode("*3\r\n:1\r\n:2\r\n:3\r\n")
    end

    test "decodes array of bulk strings" do
      input = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
      assert {:ok, [["hello", "world"]]} = Decoder.decode(input)
    end

    test "decodes array of mixed types" do
      input = "*3\r\n:1\r\n$5\r\nhello\r\n#t\r\n"
      assert {:ok, [[1, "hello", true]]} = Decoder.decode(input)
    end

    test "decodes nested array" do
      input = "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"
      assert {:ok, [[[1, 2], [3]]]} = Decoder.decode(input)
    end
  end

  describe "null decoding" do
    test "decodes null value" do
      assert {:ok, [nil]} = Decoder.decode("_\r\n")
    end
  end

  describe "boolean decoding" do
    test "decodes true" do
      assert {:ok, [true]} = Decoder.decode("#t\r\n")
    end

    test "decodes false" do
      assert {:ok, [false]} = Decoder.decode("#f\r\n")
    end
  end

  describe "double/float decoding" do
    test "decodes float value" do
      assert {:ok, [3.14]} = Decoder.decode(",3.14\r\n")
      assert {:ok, [-2.5]} = Decoder.decode(",-2.5\r\n")
      assert {:ok, [0.0]} = Decoder.decode(",0.0\r\n")
    end

    test "decodes infinity" do
      assert {:ok, [:infinity]} = Decoder.decode(",inf\r\n")
      assert {:ok, [:negative_infinity]} = Decoder.decode(",-inf\r\n")
    end

    test "decodes NaN" do
      assert {:ok, [:nan]} = Decoder.decode(",nan\r\n")
    end
  end

  describe "big number decoding" do
    test "decodes big number as integer" do
      assert {:ok, [9_999_999_999]} = Decoder.decode("(9999999999\r\n")
    end
  end

  describe "bulk error decoding" do
    test "decodes bulk error as string" do
      assert {:ok, ["Error occurred"]} = Decoder.decode("!14\r\nError occurred\r\n")
    end
  end

  describe "map decoding" do
    test "decodes empty map" do
      assert {:ok, [%{}]} = Decoder.decode("%0\r\n")
    end

    test "decodes map with string keys" do
      input = "%2\r\n$4\r\nname\r\n$4\r\nJohn\r\n$3\r\nage\r\n:30\r\n"
      assert {:ok, [%{"name" => "John", "age" => 30}]} = Decoder.decode(input)
    end

    test "decodes map with mixed key types" do
      input = "%2\r\n$4\r\nname\r\n$4\r\nJohn\r\n:1\r\n:42\r\n"
      assert {:ok, [%{"name" => "John", 1 => 42}]} = Decoder.decode(input)
    end

    test "decodes nested map" do
      input = "%1\r\n$4\r\nuser\r\n%2\r\n$4\r\nname\r\n$4\r\nJohn\r\n$3\r\nage\r\n:30\r\n"
      expected = %{"user" => %{"name" => "John", "age" => 30}}
      assert {:ok, [^expected]} = Decoder.decode(input)
    end
  end

  describe "set decoding" do
    test "decodes empty set" do
      res = MapSet.new()
      assert {:ok, [res]} = Decoder.decode("~0\r\n")
    end

    test "decodes set of integers" do
      res = MapSet.new([1, 2, 3])
      assert {:ok, [res]} = Decoder.decode("~3\r\n:1\r\n:2\r\n:3\r\n")
    end

    test "decodes set of mixed types" do
      input = "~2\r\n:1\r\n$5\r\nhello\r\n"
      res = MapSet.new([1, "hello"])
      assert {:ok, [res]} = Decoder.decode(input)
    end
  end

  describe "complex nested structures" do
    test "decodes complex nested structure" do
      # A complex structure with arrays, maps and different types
      input =
        "*3\r\n:1\r\n%2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$4\r\nflag\r\n#t\r\n~2\r\n:1\r\n:2\r\n"

      expected = [1, %{"key" => "value", "flag" => true}, MapSet.new([1, 2])]
      assert {:ok, [expected]} = Decoder.decode(input)
    end

    test "decodes multiple consecutive commands" do
      input = "+OK\r\n:42\r\n$5\r\nhello\r\n"
      assert {:ok, ["OK", 42, "hello"]} = Decoder.decode(input)
    end
  end

  describe "invalid type decoding" do
    test "handles invalid type prefix" do
      assert {:error, "invalid resp type 'X'"} = Decoder.decode("X123\r\n")
      assert {:error, "invalid resp type '?'"} = Decoder.decode("?unknown\r\n")
    end
  end
end
