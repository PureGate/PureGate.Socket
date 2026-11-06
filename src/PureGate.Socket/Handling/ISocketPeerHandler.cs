using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Handling;

public interface ISocketPeerHandler
{
  ValueTask OnConnectedAsync(SocketPeer peer, CancellationToken cancellationToken)
  {
    return ValueTask.CompletedTask;
  }

  ValueTask<SocketResponse> HandleRequestAsync(
      SocketPeer peer,
      SocketRequest request,
      CancellationToken cancellationToken);

  ValueTask<SocketResponse> HandleFastRequestAsync(
      SocketPeer peer,
      SocketFastRequest request,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketResponse.Failed(
        SocketProtocolError.UnsupportedOperation,
        "Fast request was not handled."));
  }

  ValueTask<SocketStreamResponse> HandleStreamAsync(
      SocketPeer peer,
      SocketStreamRequest request,
      CancellationToken cancellationToken)
  {
    return ValueTask.FromResult(SocketStreamResponse.Failed(
        SocketProtocolError.UnsupportedOperation,
        "Stream request was not handled."));
  }

  ValueTask HandleEventAsync(
      SocketPeer peer,
      SocketEvent socketEvent,
      CancellationToken cancellationToken)
  {
    return ValueTask.CompletedTask;
  }

  ValueTask OnDisconnectedAsync(SocketPeer peer, Exception exception)
  {
    return ValueTask.CompletedTask;
  }
}
