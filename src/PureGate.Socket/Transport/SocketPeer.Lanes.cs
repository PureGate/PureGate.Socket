using System.Collections.Concurrent;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// Handler lanes (SocketFabricOptions.HandlerLanes): requests and stream requests of chosen operations run
// on a pool of their own, with its own queue, workers and limit, so they never wait behind the general
// handlers (SocketPeer.Handlers.cs) and the general handlers never wait behind them.
//
// What stays shared with the general handlers: MaxQueuedHandlers counts the lanes' queued frames too, and
// a lane's frame returns its message credit when a worker takes it, at once rather than batched (see
// ReturnMessageCreditNow). The sending side keeps part of its message credit for lane messages (see
// MessageCredits), so lane requests can always be sent and always find a worker.
//
// Without lanes, LanePools and LaneMap are null and the only cost is a null check per handler frame.
public sealed partial class SocketPeer
{
  readonly SocketHandlerLaneMap LaneMap;
  readonly LanePool[] LanePools;
  // Workers running across every lane, for WaitForHandlersAsync.
  int ActiveLaneWorkers;

  LanePool[] CreateLanePools(SocketHandlerLaneMap map)
  {
    if (map is null)
      return null;

    var pools = new LanePool[map.Lanes.Length];
    for (var i = 0; i < pools.Length; i++)
      pools[i] = new LanePool(this, map.Lanes[i].MaxConcurrentHandlers);
    return pools;
  }

  // True when the frame belongs to a lane and was queued there.
  bool TryPushLaneFrame(SocketFrame frame)
  {
    var index = LaneMap.Find(frame.Service, frame.Operation);
    if (index < 0)
      return false;

    var pool = LanePools[index];
    pool.Queue.Enqueue(frame);
    TryStartLaneWorker(pool);
    return true;
  }

  void TryStartLaneWorker(LanePool pool)
  {
    while (!Lifetime.IsCancellationRequested)
    {
      var active = Volatile.Read(ref pool.Active);
      if (active >= pool.Limit)
        return;

      if (Interlocked.CompareExchange(ref pool.Active, active + 1, active) == active)
      {
        Interlocked.Increment(ref ActiveLaneWorkers);
        ThreadPool.UnsafeQueueUserWorkItem(pool, preferLocal: false);
        return;
      }
    }
  }

  // The lane counterpart of RunHandlerWorkerAsync.
  async Task RunLaneWorkerAsync(LanePool pool)
  {
    try
    {
      // Like every handler, lane handlers start only after OnConnectedAsync has returned.
      if (!ConnectedTask.IsCompleted)
        await ConnectedTask;

      while (pool.Queue.TryDequeue(out var frame))
      {
        Interlocked.Decrement(ref QueuedHandlerFrames);
        ReturnMessageCreditNow();
        if (Lifetime.IsCancellationRequested)
        {
          ReleaseFrame(frame);
          continue;
        }

        await DispatchHandlerFrameAsync(frame);
      }
    }
    finally
    {
      Interlocked.Decrement(ref pool.Active);
      var remaining = Interlocked.Decrement(ref ActiveLaneWorkers);

      // A frame enqueued while this worker was exiting may have seen every worker of the lane busy.
      if (!pool.Queue.IsEmpty)
        TryStartLaneWorker(pool);
      if (remaining == 0 && Volatile.Read(ref ClosedFlag) != 0 && Volatile.Read(ref ActiveHandlerWorkers) == 0)
        HandlersDrained.TrySetResult();
    }
  }

  // One lane on this connection. It is its own thread-pool work item, so starting a worker allocates nothing.
  sealed class LanePool(SocketPeer peer, int limit) : IThreadPoolWorkItem
  {
    public readonly ConcurrentQueue<SocketFrame> Queue = new();
    public readonly int Limit = limit;
    public int Active;

    public void Execute()
    {
      _ = peer.RunLaneWorkerAsync(this);
    }
  }
}
