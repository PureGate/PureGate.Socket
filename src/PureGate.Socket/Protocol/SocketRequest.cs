using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public sealed class SocketRequest(
    long messageId,
    string service,
    string operation,
    ReadOnlyMemory<byte> payload,
    string idempotencyKey = null)
{
  public long MessageId { get; } = messageId;
  public string Service { get; } = service ?? string.Empty;
  public string Operation { get; } = operation ?? string.Empty;
  public ReadOnlyMemory<byte> Payload { get; } = payload;
  public string IdempotencyKey { get; } = idempotencyKey ?? string.Empty;

  public T ReadPayload<T>(ISocketMessageCodec codec)
  {
    return codec.Deserialize<T>(Payload.Span);
  }
}
