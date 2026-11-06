using PureGate.Socket.Observability;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  async ValueTask CloseAsync(Exception exception)
  {
    if (Interlocked.Exchange(ref ClosedFlag, 1) != 0)
      return;

    State = SocketPeerState.Closed;
    Lifetime.Cancel();
    CloseWrites();
    MessageCredits.Fail(Lifetime.Token);
    DrainHandlerQueue();
    ReleaseIncomingChunks();
    foreach (var active in ActiveRequests.Values)
      active.Source.Cancel();
    ActiveRequests.Clear();
    foreach (var (messageId, pending) in PendingRequests)
    {
      pending.TrySetResult(messageId, new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.ConnectionClosed,
        ErrorMessage = "Connection closed."
      });
    }
    PendingRequests.Clear();
    foreach (var pending in PendingStreams.Values)
    {
      pending.TrySetFailed(new SocketProtocolException(
          SocketProtocolError.ConnectionClosed,
          "Connection closed."));
      pending.Stream.Complete(new SocketProtocolException(
          SocketProtocolError.ConnectionClosed,
          "Connection closed."));
      pending.ReleaseInFlight(this);
    }
    PendingStreams.Clear();
    SocketMetrics.Closed(this, exception);
    try
    {
      ObservePeerDisconnected(exception);
      await Handler.OnDisconnectedAsync(this, exception);
    }
    catch
    {
    }
    Stream.Dispose();
    Client.Dispose();
    Closed.TrySetResult();
  }

  public async ValueTask DisposeAsync()
  {
    await CloseGracefullyAsync();
    try
    {
      await Task.WhenAll(ReadTask, HeartbeatTask);
      await ConnectedTask;
      await WaitForHandlersAsync();
    }
    catch
    {
    }

    // Lifetime is deliberately not disposed: callers that were in flight when the peer closed still touch
    // it on their way out, and disposing it turned a clean connection_closed result into an
    // ObjectDisposedException. It holds no unmanaged resources.
    while (RequestCancellationPool.TryDequeue(out var cancellation))
      cancellation.Dispose();
  }

  void ThrowIfClosed()
  {
    if (State == SocketPeerState.Closed || Lifetime.IsCancellationRequested)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection is closed.");
  }

  static int ToDeadlineMilliseconds(TimeSpan timeout)
  {
    if (timeout <= TimeSpan.Zero)
      return 0;
    return Math.Min(int.MaxValue, (int)Math.Ceiling(timeout.TotalMilliseconds));
  }
}
