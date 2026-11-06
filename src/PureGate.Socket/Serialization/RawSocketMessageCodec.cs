using System.Buffers;
using System.Text;

namespace PureGate.Socket.Serialization;

// Bytes as they are, and strings as UTF-8.
public sealed class RawSocketMessageCodec : ISocketMessageCodec
{
  public static readonly RawSocketMessageCodec Shared = new();

  public void Serialize<T>(T value, IBufferWriter<byte> output)
  {
    ArgumentNullException.ThrowIfNull(output);
    switch (value)
    {
      case null:
        return;
      case byte[] bytes:
        output.Write(bytes);
        return;
      case ReadOnlyMemory<byte> memory:
        output.Write(memory.Span);
        return;
      case Memory<byte> memory:
        output.Write(memory.Span);
        return;
      case ArraySegment<byte> segment:
        output.Write(segment.AsSpan());
        return;
      case string text:
        Encoding.UTF8.GetBytes(text, output);
        return;
      default:
        throw new NotSupportedException("Raw socket codec only supports byte arrays, byte memory, and strings.");
    }
  }

  public T Deserialize<T>(ReadOnlySequence<byte> payload)
  {
    var type = typeof(T);
    if (type == typeof(byte[]))
      return (T)(object)payload.ToArray();
    if (type == typeof(ReadOnlyMemory<byte>))
      return (T)(object)new ReadOnlyMemory<byte>(payload.ToArray());
    if (type == typeof(Memory<byte>))
      return (T)(object)new Memory<byte>(payload.ToArray());
    if (type == typeof(ArraySegment<byte>))
      return (T)(object)new ArraySegment<byte>(payload.ToArray());
    if (type == typeof(string))
      return (T)(object)Encoding.UTF8.GetString(payload);

    throw new NotSupportedException("Raw socket codec only supports byte arrays, byte memory, and strings.");
  }
}
