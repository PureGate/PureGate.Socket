namespace PureGate.Socket.Security;

public readonly record struct SocketAuthenticationResult(
    bool Succeeded,
    string RemoteNodeId,
    string Error)
{
  public static SocketAuthenticationResult Success(string remoteNodeId)
  {
    return new SocketAuthenticationResult(true, remoteNodeId ?? string.Empty, null);
  }

  public static SocketAuthenticationResult Failed(string error)
  {
    return new SocketAuthenticationResult(false, string.Empty, error ?? "Authentication failed.");
  }
}
