using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public sealed class SocketRequestResult
{
  public bool Succeeded { get; init; }
  public ReadOnlyMemory<byte> Payload { get; init; }
  public string ErrorCode { get; init; }
  public string ErrorMessage { get; init; }

  public T ReadPayload<T>(ISocketMessageCodec codec)
  {
    return codec.Deserialize<T>(Payload.Span);
  }
}
