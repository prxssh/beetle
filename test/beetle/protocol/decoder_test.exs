defmodule Beetle.Protocol.DecoderTest do
  use ExUnit.Case
  doctest Beetle.Protocol.Decoder

  alias Beetle.Protocol.Decoder

  describe "decode/2" do
    test "rejects non-binary input" do
      assert {:error, "input must be a binary"} = Decoder.decode(123)
      assert {:error, "input must be a binary"} = Decoder.decode(:not_binary)
      assert {:error, "input must be a binary"} = Decoder.decode([1, 2, 3])
    end

    test "handles empty input" do
      assert {:ok, []} = Decoder.decode(<<>>)
    end
  end

  describe "Simple String decoding" do
    test "decodes a simple string" do
      assert {:ok, ["hello"]} = Decoder.decode("+hello\r\n")
    end

    test "decodes multiple simple strings" do
      assert {:ok, ["hello", "world"]} = Decoder.decode("+hello\r\n+world\r\n")
    end

    test "handles empty simple string" do
      assert {:ok, [""]} = Decoder.decode("+\r\n")
    end
  end

  describe "Simple Error decoding" do
    test "decodes a simple error" do
      assert {:ok, ["ERR unknown command"]} = Decoder.decode("-ERR unknown command\r\n")
    end
  end

  describe "Integer decoding" do
    test "decodes a positive integer" do
      assert {:ok, [42]} = Decoder.decode(":42\r\n")
    end

    test "decodes a negative integer" do
      assert {:ok, [-123]} = Decoder.decode(":-123\r\n")
    end

    test "decodes zero" do
      assert {:ok, [0]} = Decoder.decode(":0\r\n")
    end

    test "handles invalid integer format" do
      assert {:error, _} = Decoder.decode(":not_an_integer\r\n")
    end
  end

  describe "Double decoding" do
    test "decodes a float" do
      assert {:ok, [3.14]} = Decoder.decode(",3.14\r\n")
    end

    test "decodes a negative float" do
      assert {:ok, [-2.71]} = Decoder.decode(",-2.71\r\n")
    end

    test "decodes infinity" do
      assert {:ok, [:infinity]} = Decoder.decode(",inf\r\n")
    end

    test "decodes negative infinity" do
      assert {:ok, [:negative_infinity]} = Decoder.decode(",-inf\r\n")
    end

    test "decodes NaN" do
      assert {:ok, [:nan]} = Decoder.decode(",nan\r\n")
    end

    test "handles invalid float format" do
      assert {:error, _} = Decoder.decode(",not_a_float\r\n")
    end
  end

  describe "Bulk String decoding" do
    test "decodes a bulk string" do
      assert {:ok, ["hello"]} = Decoder.decode("$5\r\nhello\r\n")
    end

    test "decodes an empty bulk string" do
      assert {:ok, [""]} = Decoder.decode("$0\r\n\r\n")
    end

    test "decodes a null bulk string" do
      assert {:ok, [nil]} = Decoder.decode("$-1\r\n")
    end

    test "handles invalid bulk string length" do
      assert {:error, _} = Decoder.decode("$-2\r\n")
    end

    test "handles insufficient data" do
      assert {:error, _} = Decoder.decode("$5\r\nhel\r\n")
    end
  end

  describe "Array decoding" do
    test "decodes an empty array" do
      assert {:ok, [[]]} = Decoder.decode("*0\r\n")
    end

    test "decodes a null array" do
      assert {:ok, [nil]} = Decoder.decode("*-1\r\n")
    end

    test "decodes a simple array" do
      input = "*3\r\n$5\r\nhello\r\n$5\r\nworld\r\n:42\r\n"
      assert {:ok, [["hello", "world", 42]]} = Decoder.decode(input)
    end

    test "decodes nested arrays" do
      input = "*2\r\n*2\r\n+hello\r\n+world\r\n*1\r\n:42\r\n"
      assert {:ok, [[["hello", "world"], [42]]]} = Decoder.decode(input)
    end

    test "handles invalid array length" do
      assert {:error, _} = Decoder.decode("*-2\r\n")
    end
  end

  describe "Null decoding" do
    test "decodes a null value" do
      assert {:ok, [nil]} = Decoder.decode("_\r\n")
    end
  end

  describe "Boolean decoding" do
    test "decodes true" do
      assert {:ok, [true]} = Decoder.decode("#t\r\n")
    end

    test "decodes false" do
      assert {:ok, [false]} = Decoder.decode("#f\r\n")
    end

    test "handles invalid boolean" do
      assert {:error, _} = Decoder.decode("#invalid\r\n")
    end
  end

  describe "Big Number decoding" do
    test "decodes a big number" do
      assert {:ok, [9223372036854775807]} = Decoder.decode("(9223372036854775807\r\n")
    end
  end

  describe "Bulk Error decoding" do
    test "decodes a bulk error" do
      assert {:ok, ["Error message"]} = Decoder.decode("!13\r\nError message\r\n")
    end
  end

  describe "Map decoding" do
    test "decodes an empty map" do
      assert {:ok, [%{}]} = Decoder.decode("%0\r\n")
    end

    test "decodes a simple map" do
      input = "%2\r\n+name\r\n+John\r\n+age\r\n:30\r\n"
      assert {:ok, [%{"name" => "John", "age" => 30}]} = Decoder.decode(input)
    end

    test "decodes a complex map" do
      input = "%2\r\n+user\r\n*2\r\n+name\r\n+John\r\n+stats\r\n%2\r\n+points\r\n:100\r\n+level\r\n:5\r\n"
      expected = %{
        "user" => ["name", "John"],
        "stats" => %{"points" => 100, "level" => 5}
      }
      assert {:ok, [^expected]} = Decoder.decode(input)
    end

    test "handles invalid map count" do
      assert {:error, _} = Decoder.decode("%-1\r\n")
    end
  end

  describe "Set decoding" do
    test "decodes an empty set" do
      res = MapSet.new()
      assert {:ok, [^res]} = Decoder.decode("~0\r\n")
    end

    test "decodes a set of strings" do
      input = "~3\r\n+apple\r\n+banana\r\n+cherry\r\n"
      expected = MapSet.new(["apple", "banana", "cherry"])
      assert {:ok, [^expected]} = Decoder.decode(input)
    end

    test "decodes a set of mixed types" do
      input = "~3\r\n+apple\r\n:42\r\n,3.14\r\n"
      expected = MapSet.new(["apple", 42, 3.14])
      assert {:ok, [^expected]} = Decoder.decode(input)
    end
  end

  describe "Invalid Type decoding" do
    test "handles invalid type indicator" do
      assert {:error, _} = Decoder.decode("Xinvalid\r\n")
    end
  end

  describe "Multiple data type decoding" do
    test "decodes a mix of data types" do
      input = "+hello\r\n:42\r\n*2\r\n+world\r\n$5\r\nhello\r\n%1\r\n+key\r\n:123\r\n"
      expected = ["hello", 42, ["world", "hello"], %{"key" => 123}]
      assert {:ok, ^expected} = Decoder.decode(input)
    end
  end

  describe "Malformed input handling" do
    test "handles missing CRLF" do
      assert {:error, _} = Decoder.decode("+hello")
    end

    test "handles incomplete input" do
      assert {:error, _} = Decoder.decode("*2\r\n+hello\r\n")
    end
  end
end
