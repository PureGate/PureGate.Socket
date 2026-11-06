using PureGate.Socket.Protocol;
using System.Threading.Tasks.Sources;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  // Pooled, so one instance serves many requests over its life. A completer (the read loop, the close
  // path) can hold a reference across the moment the instance is returned and re-rented. To make a stale
  // completion impossible, the instance is armed with its request's message ID and completion atomically
  // swaps that exact ID for 0. A completer holding an old ID loses the swap and does nothing.
  sealed class PendingRequest : IValueTaskSource<SocketRequestResult>
  {
    ManualResetValueTaskSourceCore<SocketRequestResult> Source;
    long ArmedMessageId;

    // The requester deserializes the response and keeps no bytes, so its payload may be pooled. Set
    // before the request is registered, so the read loop sees the value for this request.
    public bool PooledResponse { get; set; }

    public PendingRequest()
    {
      Source.RunContinuationsAsynchronously = true;
    }

    public ValueTask<SocketRequestResult> Task => new(this, Source.Version);

    public void Arm(long messageId)
    {
      Source.Reset();
      Volatile.Write(ref ArmedMessageId, messageId);
    }

    // Called before the instance goes back to the pool. A request that never got a result (its write threw,
    // for example) is still armed; disarming it makes any completer still holding its ID fail the swap, so
    // it cannot complete the instance's next request or complete it twice.
    public void Disarm()
    {
      Interlocked.Exchange(ref ArmedMessageId, 0);
    }

    public bool TrySetResult(long messageId, SocketRequestResult result)
    {
      if (messageId == 0 || Interlocked.CompareExchange(ref ArmedMessageId, 0, messageId) != messageId)
        return false;

      Source.SetResult(result);
      return true;
    }

    // Used only by the timeout and cancellation callbacks, whose registrations are disposed before the
    // instance goes back to the pool, so the currently armed ID is always theirs.
    void TrySetCurrentResult(SocketRequestResult result)
    {
      TrySetResult(Volatile.Read(ref ArmedMessageId), result);
    }

    public void TrySetTimeout()
    {
      TrySetCurrentResult(new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.Timeout,
        ErrorMessage = "Request timed out."
      });
    }

    public void TrySetCancelled()
    {
      TrySetCurrentResult(new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.Cancelled,
        ErrorMessage = "Request was cancelled."
      });
    }

    public SocketRequestResult GetResult(short token)
    {
      return Source.GetResult(token);
    }

    public ValueTaskSourceStatus GetStatus(short token)
    {
      return Source.GetStatus(token);
    }

    public void OnCompleted(
        Action<object> continuation,
        object state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
      Source.OnCompleted(continuation, state, token, flags);
    }
  }

  sealed class PendingStream
  {
    readonly TaskCompletionSource<Stream> Source = new(TaskCreationOptions.RunContinuationsAsynchronously);
    int Started;
    int Released;

    public PendingStream(SocketPullStream stream)
    {
      Stream = stream;
    }

    public SocketPullStream Stream { get; }
    public Task<Stream> Task => Source.Task;
    public bool IsStarted => Volatile.Read(ref Started) != 0;

    public void TrySetStarted()
    {
      Interlocked.Exchange(ref Started, 1);
      Source.TrySetResult(Stream);
    }

    public void TrySetFailed(Exception exception)
    {
      Source.TrySetException(exception);
    }

    public void TrySetCancelled()
    {
      Source.TrySetCanceled();
    }

    public void ReleaseInFlight(SocketPeer peer)
    {
      if (Interlocked.Exchange(ref Released, 1) == 0)
        peer.ReleaseInFlight();
    }
  }

  PendingRequest RentPendingRequest(long messageId, bool pooledResponse)
  {
    if (!PendingRequestPool.TryDequeue(out var pending))
      pending = new PendingRequest();
    pending.PooledResponse = pooledResponse;
    pending.Arm(messageId);
    return pending;
  }

  void ReturnPendingRequest(PendingRequest pending)
  {
    if (pending is null)
      return;

    pending.Disarm();
    PendingRequestPool.Enqueue(pending);
  }

  CancellationTokenSource RentRequestCancellation()
  {
    return RequestCancellationPool.TryDequeue(out var source)
        ? source
        : new CancellationTokenSource();
  }

  void ReturnRequestCancellation(CancellationTokenSource source)
  {
    if (source is null)
      return;

    source.CancelAfter(Timeout.InfiniteTimeSpan);
    if (!source.TryReset())
    {
      source.Dispose();
      return;
    }

    RequestCancellationPool.Enqueue(source);
  }
}
