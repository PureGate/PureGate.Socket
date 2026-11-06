using System.Buffers;

namespace PureGate.Socket.Serialization;

public static class SocketMessageCodecExtensions
{
  // Serializes to a new array of exactly the encoded length.
  public static byte[] Serialize<T>(this ISocketMessageCodec codec, T value)
  {
    ArgumentNullException.ThrowIfNull(codec);
    return SocketPayloadWriter.SerializeToArray(codec, value);
  }

  public static T Deserialize<T>(this ISocketMessageCodec codec, ReadOnlyMemory<byte> payload)
  {
    ArgumentNullException.ThrowIfNull(codec);
    return codec.Deserialize<T>(new ReadOnlySequence<byte>(payload));
  }
}
