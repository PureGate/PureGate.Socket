using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public sealed class SocketEvent(
    string service,
    string operation,
    ReadOnlyMemory<byte> payload)
{
  public string Service { get; } = service ?? string.Empty;
  public string Operation { get; } = operation ?? string.Empty;
  public ReadOnlyMemory<byte> Payload { get; } = payload;

  public T ReadPayload<T>(ISocketMessageCodec codec)
  {
    return codec.Deserialize<T>(Payload.Span);
  }
}
