using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// Runs request, stream and event handlers off the read loop.
//
// At most MaxConcurrentHandlers workers run at once, and at most MaxQueuedHandlers frames wait for one.
// Workers are started on demand and each drains the queue before it exits, so one incoming frame wakes
// at most one worker. (The previous design kept MaxConcurrentHandlers workers parked on a channel; every
// frame woke all of them and the losers allocated a new wait operation each time they re-parked.)
public sealed partial class SocketPeer
{
  readonly ConcurrentQueue<SocketFrame> HandlerQueue = new();
  readonly TaskCompletionSource HandlersDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);
  HandlerWorkItem HandlerWork;
  int QueuedHandlerFrames;
  int ActiveHandlerWorkers;

  bool TryEnqueueHandlerFrame(SocketFrame frame)
  {
    if (Interlocked.Increment(ref QueuedHandlerFrames) > Options.MaxQueuedHandlers)
    {
      Interlocked.Decrement(ref QueuedHandlerFrames);
      return false;
    }

    HandlerQueue.Enqueue(frame);
    TryStartHandlerWorker();
    return true;
  }

  void TryStartHandlerWorker()
  {
    while (!Lifetime.IsCancellationRequested)
    {
      var active = Volatile.Read(ref ActiveHandlerWorkers);
      if (active >= Options.MaxConcurrentHandlers)
        return;

      if (Interlocked.CompareExchange(ref ActiveHandlerWorkers, active + 1, active) == active)
      {
        ThreadPool.UnsafeQueueUserWorkItem(HandlerWork ??= new HandlerWorkItem(this), preferLocal: false);
        return;
      }
    }
  }

  async Task RunHandlerWorkerAsync()
  {
    try
    {
      while (HandlerQueue.TryDequeue(out var frame))
      {
        Interlocked.Decrement(ref QueuedHandlerFrames);
        if (Lifetime.IsCancellationRequested)
        {
          frame.ReleasePayload();
          continue;
        }

        await DispatchHandlerFrameAsync(frame);
      }
    }
    finally
    {
      var remaining = Interlocked.Decrement(ref ActiveHandlerWorkers);

      // A frame enqueued while this worker was exiting may have seen every worker busy; pick it up.
      if (!HandlerQueue.IsEmpty)
        TryStartHandlerWorker();
      if (remaining == 0 && Volatile.Read(ref ClosedFlag) != 0)
        HandlersDrained.TrySetResult();
    }
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask DispatchHandlerFrameAsync(SocketFrame frame)
  {
    try
    {
      switch (frame.Kind)
      {
        case SocketMessageKind.Request:
          await DispatchRequestAsync(frame);
          break;
        case SocketMessageKind.FastRequest:
          await DispatchFastRequestAsync(frame);
          break;
        case SocketMessageKind.StreamRequest:
          await DispatchStreamRequestAsync(frame);
          break;
        case SocketMessageKind.Event:
          await DispatchEventAsync(frame);
          break;
      }
    }
    catch (Exception ex) when (!Lifetime.IsCancellationRequested)
    {
      // Dispatch methods turn every handler failure into a response, so what reaches this point is a
      // transport failure while answering. The connection is unusable; close it.
      await CloseAsync(ex);
    }
    catch
    {
      // The connection is closing; there is nobody left to answer.
    }
    finally
    {
      frame.ReleasePayload();
    }
  }

  void DrainHandlerQueue()
  {
    while (HandlerQueue.TryDequeue(out var frame))
    {
      Interlocked.Decrement(ref QueuedHandlerFrames);
      frame.ReleasePayload();
    }
  }

  // Completes once every running handler has finished. Only meaningful after the peer has closed.
  Task WaitForHandlersAsync()
  {
    if (Volatile.Read(ref ActiveHandlerWorkers) == 0)
      HandlersDrained.TrySetResult();
    return HandlersDrained.Task;
  }

  sealed class HandlerWorkItem(SocketPeer peer) : IThreadPoolWorkItem
  {
    public void Execute()
    {
      _ = peer.RunHandlerWorkerAsync();
    }
  }
}
