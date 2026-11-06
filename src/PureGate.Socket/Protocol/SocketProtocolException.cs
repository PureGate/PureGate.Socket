namespace PureGate.Socket.Protocol;

public sealed class SocketProtocolException(string errorCode, string message) : Exception(message)
{
  public string ErrorCode { get; } = errorCode ?? SocketProtocolError.ProtocolError;
}
