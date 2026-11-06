namespace PureGate.Socket.Security;

public sealed class SocketAuthorizationResult
{
  public static readonly SocketAuthorizationResult Allowed = new(true, null);

  SocketAuthorizationResult(bool succeeded, string error)
  {
    Succeeded = succeeded;
    Error = error ?? string.Empty;
  }

  public bool Succeeded { get; }
  public string Error { get; }

  public static SocketAuthorizationResult Denied(string error)
  {
    return new SocketAuthorizationResult(false, error);
  }
}
