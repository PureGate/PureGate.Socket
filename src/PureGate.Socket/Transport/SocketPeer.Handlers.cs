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

  // Event ordering (SocketEventOrdering). Events are routed through lanes: a lane lets one event run at a
  // time and holds the rest in arrival order. Sequential mode has one lane per connection, PerOperation
  // one per service and operation. A held event still counts as queued (against MaxQueuedHandlers and
  // the sender's message credit) and goes through the same worker pool, so concurrency and flow control
  // limits stay exact. Requests never wait behind events.
  readonly Lock LaneSync = new();
  readonly Dictionary<(string Service, string Operation), EventLane> EventLanes = new();

  bool TryEnqueueHandlerFrame(SocketFrame frame)
  {
    if (Interlocked.Increment(ref QueuedHandlerFrames) > Options.MaxQueuedHandlers)
    {
      Interlocked.Decrement(ref QueuedHandlerFrames);
      return false;
    }

    if (frame.Kind == SocketMessageKind.Event && HoldInLane(frame))
      return true;

    PushHandlerFrame(frame);
    return true;
  }

  void PushHandlerFrame(SocketFrame frame)
  {
    HandlerQueue.Enqueue(frame);
    TryStartHandlerWorker();
  }

  (string Service, string Operation) LaneKey(SocketFrame frame)
  {
    return Options.EventOrdering == SocketEventOrdering.PerOperation
        ? (frame.Service, frame.Operation)
        : (string.Empty, string.Empty);
  }

  // True when the event has to wait for an earlier one in its lane; false when it may run now (its lane
  // is then marked busy until ReleaseLane).
  bool HoldInLane(SocketFrame frame)
  {
    if (Options.EventOrdering == SocketEventOrdering.Concurrent)
      return false;

    var key = LaneKey(frame);
    lock (LaneSync)
    {
      if (EventLanes.TryGetValue(key, out var lane))
      {
        lane.Waiting.Enqueue(frame);
        return true;
      }

      EventLanes[key] = new EventLane();
      return false;
    }
  }

  // An event finished: start the next one in its lane, or retire the lane.
  void ReleaseLane(SocketFrame frame)
  {
    if (Options.EventOrdering == SocketEventOrdering.Concurrent)
      return;

    var key = LaneKey(frame);
    SocketFrame next;
    lock (LaneSync)
    {
      if (!EventLanes.TryGetValue(key, out var lane))
        return;

      if (!lane.Waiting.TryDequeue(out next))
      {
        EventLanes.Remove(key);
        return;
      }
    }

    PushHandlerFrame(next);
  }

  void DrainEventLanes()
  {
    lock (LaneSync)
    {
      foreach (var lane in EventLanes.Values)
      {
        while (lane.Waiting.TryDequeue(out var frame))
        {
          Interlocked.Decrement(ref QueuedHandlerFrames);
          frame.ReleasePayload();
        }
      }

      EventLanes.Clear();
    }
  }

  sealed class EventLane
  {
    public Queue<SocketFrame> Waiting { get; } = new();
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
        // The queue slot is free again: hand the sender its credit back. Credits are returned on pickup,
        // not on completion, so they track queue space; MaxConcurrentHandlers bounds what is running.
        Interlocked.Decrement(ref QueuedHandlerFrames);
        ReturnMessageCredit();
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
        case SocketMessageKind.StreamRequest:
          await DispatchStreamRequestAsync(frame);
          break;
        case SocketMessageKind.Event:
          try
          {
            await DispatchEventAsync(frame);
          }
          finally
          {
            ReleaseLane(frame);
          }

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

    DrainEventLanes();
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
