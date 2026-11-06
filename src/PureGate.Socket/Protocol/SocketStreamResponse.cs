namespace PureGate.Socket.Protocol;

public sealed class SocketStreamResponse
{
  public bool Succeeded { get; init; }
  public Stream Stream { get; init; }
  public string ErrorCode { get; init; }
  public string ErrorMessage { get; init; }

  public static SocketStreamResponse Success(Stream stream)
  {
    return new SocketStreamResponse
    {
      Succeeded = true,
      Stream = stream ?? Stream.Null
    };
  }

  public static SocketStreamResponse Failed(string errorCode, string errorMessage)
  {
    return new SocketStreamResponse
    {
      Succeeded = false,
      ErrorCode = errorCode ?? SocketProtocolError.HandlerFailed,
      ErrorMessage = errorMessage ?? string.Empty
    };
  }
}
