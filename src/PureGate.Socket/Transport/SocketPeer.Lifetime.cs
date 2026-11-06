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
    HandlerQueue.Writer.TryComplete(exception);
    DrainHandlerQueue();
    IncomingChunks.Clear();
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
      pending.ReleaseInFlight(InFlightRequests);
    }
    PendingStreams.Clear();
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

  void DrainHandlerQueue()
  {
    while (HandlerQueue.Reader.TryRead(out var frame))
      frame.ReleasePayload();
  }

  public async ValueTask DisposeAsync()
  {
    await CloseGracefullyAsync();
    try
    {
      await Task.WhenAll(ReadTask, HeartbeatTask);
      await ConnectedTask;
      await Task.WhenAll(HandlerTasks);
    }
    catch
    {
    }

    // Lifetime, WriteLock and InFlightRequests are deliberately not disposed: callers that were in flight
    // when the peer closed still touch them on their way out, and disposing them turned a clean
    // connection_closed result into an ObjectDisposedException. None of them hold unmanaged resources.
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
