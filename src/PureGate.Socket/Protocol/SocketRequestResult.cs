using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public sealed class SocketRequestResult
{
  // The payload arrived compressed; the requester decompresses it on its own thread.
  internal bool PayloadCompressed { get; init; }

  public bool Succeeded { get; init; }
  public ReadOnlyMemory<byte> Payload { get; init; }
  public string ErrorCode { get; init; }
  public string ErrorMessage { get; init; }

  public T ReadPayload<T>(ISocketMessageCodec codec)
  {
    return codec.Deserialize<T>(Payload.Span);
  }
}
