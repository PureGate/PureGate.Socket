namespace PureGate.Socket.Transport;

// The memory one connection may hold on behalf of its remote peer: chunked messages being reassembled,
// received requests, stream requests and events until their handler finishes, decompressed payloads,
// and stream data the application has not read yet. Message credits bound how many messages a peer can
// queue and stream windows bound each stream, but neither bounds their sizes added up; this does.
sealed class SocketMemoryBudget(long limit)
{
  long Used;

  public long Limit => limit;
  public long InUse => Interlocked.Read(ref Used);
  public long Available => Math.Max(0, limit - InUse);

  public bool TryReserve(long bytes)
  {
    if (bytes <= 0)
      return true;

    while (true)
    {
      var current = Interlocked.Read(ref Used);
      if (current + bytes > limit)
        return false;
      if (Interlocked.CompareExchange(ref Used, current + bytes, current) == current)
        return true;
    }
  }

  public void Release(long bytes)
  {
    if (bytes > 0)
      Interlocked.Add(ref Used, -bytes);
  }
}
