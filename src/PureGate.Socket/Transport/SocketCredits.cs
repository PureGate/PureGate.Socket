using PureGate.Socket.Observability;
namespace PureGate.Socket.Transport;

// Connection-level message credits: how many more handler-bound messages (requests, fast requests,
// events, stream requests) the remote has room to queue. Each send takes one credit; the remote returns
// credits as its handlers pick messages up. A sender with no credit waits instead of overrunning the
// remote's queue.
//
// Taking a credit is a single compare-and-swap while credit is available. Only a sender that has to wait
// takes the lock and allocates. Waiters are served in arrival order, and a sender never overtakes a
// waiter, so a burst cannot starve the callers that were already waiting.
//
// With handler lanes, the last `reserve` credits are kept for lane messages (AcquireReservedAsync): general
// messages (AcquireAsync) wait while no more than the reserve is left. Credit returns to one pool however it
// was spent, so the remote needs to know nothing about the split. Lane senders are served before general
// waiters and do not queue behind them, so general messages stuck at a busy remote can never keep a lane
// message from being sent.
sealed class MessageCredits(long initial, long reserve = 0)
{
  readonly Lock Sync = new();
  readonly LinkedList<TaskCompletionSource> Waiters = new();
  readonly LinkedList<TaskCompletionSource> ReservedWaiters = new();
  readonly long Reserve = reserve;
  long Available = initial;
  // Every waiter, general and lane.
  int WaiterCount;
  int ReservedWaiterCount;
  bool Failed;
  CancellationToken FailureToken;

  public ValueTask AcquireAsync(CancellationToken cancellationToken)
  {
    return Volatile.Read(ref WaiterCount) == 0 && TryTake(Reserve)
        ? ValueTask.CompletedTask
        : AcquireSlowAsync(reserved: false, cancellationToken);
  }

  // For a message that runs in a handler lane at the remote: may use the reserve, and does not wait behind
  // general waiters.
  public ValueTask AcquireReservedAsync(CancellationToken cancellationToken)
  {
    return Volatile.Read(ref ReservedWaiterCount) == 0 && TryTake(0)
        ? ValueTask.CompletedTask
        : AcquireSlowAsync(reserved: true, cancellationToken);
  }

  public void Release(long count)
  {
    if (count <= 0)
      return;

    Interlocked.Add(ref Available, count);
    // Interlocked.Add is a full fence, and a waiter raises WaiterCount (also a full fence) before checking
    // Available, so either this sees the waiter or the waiter sees the credit.
    if (Volatile.Read(ref WaiterCount) != 0)
    {
      lock (Sync)
        GrantWaiters();
    }
  }

  // Wakes every waiter with cancellation. Called when the connection closes.
  public void Fail(CancellationToken lifetime)
  {
    lock (Sync)
    {
      Failed = true;
      FailureToken = lifetime;
      foreach (var waiter in Waiters)
        waiter.TrySetCanceled(lifetime);
      foreach (var waiter in ReservedWaiters)
        waiter.TrySetCanceled(lifetime);
      Waiters.Clear();
      ReservedWaiters.Clear();
      Volatile.Write(ref WaiterCount, 0);
      Volatile.Write(ref ReservedWaiterCount, 0);
    }
  }

  async ValueTask AcquireSlowAsync(bool reserved, CancellationToken cancellationToken)
  {
    cancellationToken.ThrowIfCancellationRequested();
    TaskCompletionSource waiter;
    LinkedListNode<TaskCompletionSource> node;
    lock (Sync)
    {
      if (Failed)
        throw new OperationCanceledException(FailureToken);

      waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      node = (reserved ? ReservedWaiters : Waiters).AddLast(waiter);
      if (reserved)
        Interlocked.Increment(ref ReservedWaiterCount);
      Interlocked.Increment(ref WaiterCount);
      GrantWaiters();
    }

    if (waiter.Task.IsCompleted)
    {
      await waiter.Task;
      return;
    }

    SocketMetrics.CreditWaited();

    using var registration = cancellationToken.UnsafeRegister(
        static state =>
        {
          var (credits, pendingNode, token) = ((MessageCredits, LinkedListNode<TaskCompletionSource>, CancellationToken))state;
          credits.Cancel(pendingNode, token);
        },
        (this, node, cancellationToken));
    await waiter.Task;
  }

  void Cancel(LinkedListNode<TaskCompletionSource> node, CancellationToken cancellationToken)
  {
    lock (Sync)
    {
      if (node.List is not { } list)
        return;

      list.Remove(node);
      if (ReferenceEquals(list, ReservedWaiters))
        Interlocked.Decrement(ref ReservedWaiterCount);
      Interlocked.Decrement(ref WaiterCount);
    }

    node.Value.TrySetCanceled(cancellationToken);
  }

  // Runs under Sync. Hands credits to waiters in arrival order, lane waiters first.
  void GrantWaiters()
  {
    while (ReservedWaiters.First is { } first && TryTake(0))
    {
      ReservedWaiters.RemoveFirst();
      Interlocked.Decrement(ref ReservedWaiterCount);
      Interlocked.Decrement(ref WaiterCount);
      if (!first.Value.TrySetResult())
        Interlocked.Increment(ref Available);
    }

    while (Waiters.First is { } first && TryTake(Reserve))
    {
      Waiters.RemoveFirst();
      Interlocked.Decrement(ref WaiterCount);
      if (!first.Value.TrySetResult())
        Interlocked.Increment(ref Available);
    }
  }

  // Takes one credit if more than `floor` are left.
  bool TryTake(long floor)
  {
    while (true)
    {
      var available = Volatile.Read(ref Available);
      if (available <= floor)
        return false;
      if (Interlocked.CompareExchange(ref Available, available - 1, available) == available)
        return true;
    }
  }
}

// A stream producer's send window: how many more bytes the consumer has room for. The consumer grants
// more as its application reads. One producer per stream, so there is at most one waiter.
sealed class StreamSendWindow(long initial)
{
  readonly Lock Sync = new();
  long Available = initial;
  TaskCompletionSource Waiter;

  // True when the producer has used up its window and its next send will wait for a grant.
  public bool IsExhausted => Volatile.Read(ref Available) <= 0;

  public void Grant(long bytes)
  {
    Interlocked.Add(ref Available, bytes);
    TaskCompletionSource waiter;
    lock (Sync)
    {
      waiter = Waiter;
      Waiter = null;
    }

    waiter?.TrySetResult();
  }

  // Takes up to `wanted` bytes of window, waiting until at least one byte is available.
  public async ValueTask<int> TakeAsync(int wanted, CancellationToken cancellationToken)
  {
    while (true)
    {
      var taken = TryTake(wanted);
      if (taken > 0)
        return taken;

      TaskCompletionSource waiter;
      lock (Sync)
      {
        if (Volatile.Read(ref Available) > 0)
          continue;
        waiter = Waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      }

      await waiter.Task.WaitAsync(cancellationToken);
    }
  }

  int TryTake(int wanted)
  {
    while (true)
    {
      var available = Volatile.Read(ref Available);
      if (available <= 0)
        return 0;

      var taken = (int)Math.Min(wanted, available);
      if (Interlocked.CompareExchange(ref Available, available - taken, available) == available)
        return taken;
    }
  }
}
