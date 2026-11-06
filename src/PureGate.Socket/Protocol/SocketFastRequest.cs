using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public readonly struct SocketFastRequest(
    long messageId,
    int operationId,
    ReadOnlyMemory<byte> payload)
{
  public long MessageId { get; } = messageId;
  public int OperationId { get; } = operationId;
  public ReadOnlyMemory<byte> Payload { get; } = payload;

  public T ReadPayload<T>(ISocketMessageCodec codec)
  {
    return codec.Deserialize<T>(Payload.Span);
  }
}
