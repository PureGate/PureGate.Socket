using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Handling;

// The callbacks of one connection run in this order:
//
// 1. OnConnectedAsync, once.
// 2. HandleRequestAsync, HandleStreamAsync and HandleEventAsync, only after OnConnectedAsync has returned.
//    What the remote sends meanwhile is held, so per-connection state set up in OnConnectedAsync is always
//    there when they run.
// 3. OnDisconnectedAsync, once, and only after OnConnectedAsync has returned. When the connection closes
//    while OnConnectedAsync is running, its cancellation token is cancelled and OnDisconnectedAsync follows
//    when it returns. Handlers still running at that point see their tokens cancelled.
//
// Any callback may close or dispose its peer. DisposeAsync does not wait for callbacks still running,
// including the one it was called from: they see their cancellation tokens cancelled and finish on their
// own. Await SocketPeer.Completion to know OnDisconnectedAsync has run, and SocketPeer.WaitForHandlersAsync
// to know every callback has returned.
//
// OnConnectedAsync can send requests, streams and events on the peer and wait for their results. It must not
// wait for something only a handler of this connection can bring about: handlers start after it returns. In
// particular, when both sides wait in OnConnectedAsync for a response from the other, each side's request
// is held until its own callback returns, so both wait until the requests time out.
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
