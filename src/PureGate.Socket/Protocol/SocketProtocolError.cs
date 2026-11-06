namespace PureGate.Socket.Protocol;

public static class SocketProtocolError
{
  public const string Timeout = "timeout";
  public const string Cancelled = "cancelled";
  public const string Unauthorized = "unauthorized";
  public const string UnsupportedVersion = "unsupported_version";
  public const string UnsupportedOperation = "unsupported_operation";
  public const string PayloadTooLarge = "payload_too_large";
  public const string ProtocolError = "protocol_error";
  public const string HandlerFailed = "handler_failed";
  public const string ConnectionClosed = "connection_closed";

  // The work was rejected before it started because a local or remote limit was reached.
  // Nothing was executed, so the call is safe to retry after backing off.
  public const string Overloaded = "overloaded";
}
