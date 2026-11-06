using PureGate.Socket.Protocol;
using System.Threading.Tasks.Sources;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  sealed class PendingRequest : IValueTaskSource<SocketRequestResult>
  {
    ManualResetValueTaskSourceCore<SocketRequestResult> Source;
    int Completed;

    public PendingRequest()
    {
      Source.RunContinuationsAsynchronously = true;
    }

    public ValueTask<SocketRequestResult> Task => new(this, Source.Version);

    public void Reset()
    {
      Completed = 0;
      Source.Reset();
    }

    public void TrySetResult(SocketRequestResult result)
    {
      if (Interlocked.CompareExchange(ref Completed, 1, 0) == 0)
        Source.SetResult(result);
    }

    public void TrySetTimeout()
    {
      TrySetResult(new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.Timeout,
        ErrorMessage = "Request timed out."
      });
    }

    public void TrySetCancelled()
    {
      TrySetResult(new SocketRequestResult
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

    public void ReleaseInFlight(SemaphoreSlim inFlightRequests)
    {
      if (Interlocked.Exchange(ref Released, 1) == 0)
        inFlightRequests.Release();
    }
  }

  PendingRequest RentPendingRequest()
  {
    if (!PendingRequestPool.TryDequeue(out var pending))
      pending = new PendingRequest();
    pending.Reset();
    return pending;
  }

  void ReturnPendingRequest(PendingRequest pending)
  {
    if (pending is not null)
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
