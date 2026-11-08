using PureGate.Socket.Observability;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  const int ConnectStateConnecting = 0;
  const int ConnectStateConnected = 1;
  const int ConnectStateClosedWhileConnecting = 2;

  // Marks the async flow of this peer's OnConnectedAsync and OnDisconnectedAsync (see DisposeAsync). A
  // marker rather than the peer: tasks started inside a callback inherit the value, and must not keep the
  // peer alive.
  static readonly AsyncLocal<object> LifecycleCallback = new();
  readonly object LifecycleCallbackMarker = new();

  // Whether OnConnectedAsync has returned (see NotifyConnectedAsync and CloseAsync).
  int ConnectState;
  int DisconnectCallbackRunning;
  Exception CloseException;

  // True inside this peer's OnConnectedAsync or OnDisconnectedAsync while it runs, including in tasks it
  // started. The flow is marked, and the callback has not returned yet.
  bool InLifecycleCallback =>
      LifecycleCallback.Value == LifecycleCallbackMarker
      && (!ConnectedTask.IsCompleted || Volatile.Read(ref DisconnectCallbackRunning) != 0);

  async ValueTask CloseAsync(Exception exception)
  {
    if (Interlocked.Exchange(ref ClosedFlag, 1) != 0)
      return;

    State = SocketPeerState.Closed;
    Lifetime.Cancel();
    CloseWrites();
    MessageCredits.Fail(Lifetime.Token);
    DrainHandlerQueue();
    SignalHandlersDrainedIfIdle();
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

    // OnDisconnectedAsync must not overtake OnConnectedAsync. While that is still running, this close does
    // not wait for it (the callback may be what is closing the peer): it releases the socket now and leaves
    // OnDisconnectedAsync to NotifyConnectedAsync, which calls it once the callback returns. The callback
    // sees its cancellation token cancelled.
    CloseException = exception;
    if (Interlocked.CompareExchange(ref ConnectState, ConnectStateClosedWhileConnecting, ConnectStateConnecting) == ConnectStateConnecting)
    {
      ReleaseConnection();
      return;
    }

    await NotifyDisconnectedAsync();
    ReleaseConnection();
    Closed.TrySetResult();
  }

  // Runs once per peer, from CloseAsync or (when the peer closed during OnConnectedAsync) from
  // NotifyConnectedAsync.
  async ValueTask NotifyDisconnectedAsync()
  {
    LifecycleCallback.Value = LifecycleCallbackMarker;
    Volatile.Write(ref DisconnectCallbackRunning, 1);
    try
    {
      ObservePeerDisconnected(CloseException);
      await Handler.OnDisconnectedAsync(this, CloseException);
    }
    catch
    {
    }
    finally
    {
      Volatile.Write(ref DisconnectCallbackRunning, 0);
    }
  }

  void ReleaseConnection()
  {
    Stream.Dispose();
    Connection.Dispose();
  }

  // Closes the peer and waits for its read loop and heartbeat to stop. It does not wait for handler
  // callbacks still running (OnConnectedAsync, request, stream and event handlers): they see their
  // cancellation tokens cancelled and finish on their own. That also lets a handler dispose its own peer.
  // Completion completes once OnDisconnectedAsync has run; WaitForHandlersAsync once every callback has.
  public async ValueTask DisposeAsync()
  {
    await CloseGracefullyAsync();

    // From inside this peer's OnConnectedAsync or OnDisconnectedAsync, the read loop or the heartbeat may be
    // the task running that callback, and it finishes only after this returns, so waiting would never end.
    // The peer is closed by now, and both finish on their own once the callback returns.
    if (!InLifecycleCallback)
    {
      // Each on its own: both usually end cancelled, and that must not skip waiting for the other.
      await WaitQuietlyAsync(ReadTask);
      await WaitQuietlyAsync(HeartbeatTask);
    }

    // Lifetime is deliberately not disposed: callers that were in flight when the peer closed still touch
    // it on their way out, and disposing it turned a clean connection_closed result into an
    // ObjectDisposedException. It holds no unmanaged resources.
    while (RequestCancellationPool.TryDequeue(out var cancellation))
      cancellation.Dispose();
  }

  static async Task WaitQuietlyAsync(Task task)
  {
    try
    {
      await task;
    }
    catch
    {
      // Only that the task has ended matters here, not how.
    }
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
