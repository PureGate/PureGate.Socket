using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public sealed class SocketResponse
{
  public bool Succeeded { get; init; }
  public ReadOnlyMemory<byte> Payload { get; init; } = ReadOnlyMemory<byte>.Empty;
  public string ErrorCode { get; init; }
  public string ErrorMessage { get; init; }

  public static SocketResponse Success()
  {
    return new SocketResponse { Succeeded = true };
  }

  public static SocketResponse Success<T>(ISocketMessageCodec codec, T payload)
  {
    return new SocketResponse
    {
      Succeeded = true,
      Payload = codec.Serialize(payload)
    };
  }

  public static SocketResponse Failed(string errorCode, string errorMessage)
  {
    return new SocketResponse
    {
      Succeeded = false,
      ErrorCode = errorCode ?? SocketProtocolError.HandlerFailed,
      ErrorMessage = errorMessage ?? string.Empty
    };
  }
}
