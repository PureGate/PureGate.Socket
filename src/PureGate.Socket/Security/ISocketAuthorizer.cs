using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Security;

public interface ISocketAuthorizer
{
  ValueTask<SocketAuthorizationResult> AuthorizeRequestAsync(
      SocketPeer peer,
      SocketRequest request,
      CancellationToken cancellationToken);

  ValueTask<SocketAuthorizationResult> AuthorizeStreamAsync(
      SocketPeer peer,
      SocketStreamRequest request,
      CancellationToken cancellationToken);

  ValueTask<SocketAuthorizationResult> AuthorizeEventAsync(
      SocketPeer peer,
      SocketEvent socketEvent,
      CancellationToken cancellationToken);
}
