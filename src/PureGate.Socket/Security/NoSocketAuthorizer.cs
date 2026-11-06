using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Security;

public sealed class NoSocketAuthorizer : ISocketAuthorizer
{
  public static readonly NoSocketAuthorizer Shared = new();

  public ValueTask<SocketAuthorizationResult> AuthorizeRequestAsync(
      SocketPeer peer,
      SocketRequest request,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketAuthorizationResult.Allowed);
  }

  public ValueTask<SocketAuthorizationResult> AuthorizeFastRequestAsync(
      SocketPeer peer,
      SocketFastRequest request,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketAuthorizationResult.Allowed);
  }

  public ValueTask<SocketAuthorizationResult> AuthorizeStreamAsync(
      SocketPeer peer,
      SocketStreamRequest request,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketAuthorizationResult.Allowed);
  }

  public ValueTask<SocketAuthorizationResult> AuthorizeEventAsync(
      SocketPeer peer,
      SocketEvent socketEvent,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketAuthorizationResult.Allowed);
  }
}
