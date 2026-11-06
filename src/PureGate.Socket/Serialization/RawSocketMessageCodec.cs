using System.Text;

namespace PureGate.Socket.Serialization;

public sealed class RawSocketMessageCodec : ISocketMessageCodec
{
  public static readonly RawSocketMessageCodec Shared = new();

  public byte[] Serialize<T>(T value)
  {
    return value switch
    {
      null => [],
      byte[] bytes => bytes,
      ReadOnlyMemory<byte> memory => memory.ToArray(),
      Memory<byte> memory => memory.ToArray(),
      string text => Encoding.UTF8.GetBytes(text),
      _ => throw new NotSupportedException("Raw socket codec only supports byte arrays, byte memory, and strings.")
    };
  }

  public T Deserialize<T>(ReadOnlySpan<byte> payload)
  {
    var type = typeof(T);
    if (type == typeof(byte[]))
      return (T)(object)payload.ToArray();
    if (type == typeof(ReadOnlyMemory<byte>))
      return (T)(object)new ReadOnlyMemory<byte>(payload.ToArray());
    if (type == typeof(Memory<byte>))
      return (T)(object)new Memory<byte>(payload.ToArray());
    if (type == typeof(string))
      return (T)(object)Encoding.UTF8.GetString(payload);

    throw new NotSupportedException("Raw socket codec only supports byte arrays, byte memory, and strings.");
  }
}
