using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Handling;

sealed class EmptySocketPeerHandler : ISocketPeerHandler
{
  public static readonly EmptySocketPeerHandler Shared = new();

  public ValueTask<SocketResponse> HandleRequestAsync(
      SocketPeer peer,
      SocketRequest request,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketResponse.Failed(
        SocketProtocolError.UnsupportedOperation,
        "No socket request handler is configured."));
  }
}
