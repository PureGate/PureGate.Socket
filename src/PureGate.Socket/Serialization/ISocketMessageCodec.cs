namespace PureGate.Socket.Serialization;

public interface ISocketMessageCodec
{
  byte[] Serialize<T>(T value);
  T Deserialize<T>(ReadOnlySpan<byte> payload);
}
