namespace PureGate.Socket.Transport;

// A point-in-time view of one connection: the per-connection state that must drain once traffic stops
// (checked by the soak tests), plus totals read by SocketMetrics. Fields are read without locking, so a
// busy connection gives an approximate picture.
internal readonly record struct SocketPeerDiagnostics(
    int InFlight,
    int PendingRequests,
    int PendingStreams,
    int ActiveRequests,
    int OutgoingStreams,
    int QueuedHandlers,
    int ActiveHandlerWorkers,
    int ActiveWriters,
    int IncomingChunks,
    long IncomingChunkBytes,
    int EventLanes,
    long MemoryInUse,
    long BytesSent,
    long BytesReceived)
{
  public bool IsIdle =>
      InFlight == 0
      && PendingRequests == 0
      && PendingStreams == 0
      && ActiveRequests == 0
      && OutgoingStreams == 0
      && QueuedHandlers == 0
      && ActiveHandlerWorkers == 0
      && ActiveWriters == 0
      && IncomingChunks == 0
      && IncomingChunkBytes == 0
      && EventLanes == 0
      && MemoryInUse == 0;
}

public sealed partial class SocketPeer
{
  internal SocketPeerDiagnostics Diagnostics
  {
    get
    {
      int lanes;
      lock (LaneSync)
        lanes = EventLanes.Count;

      return new SocketPeerDiagnostics(
          Volatile.Read(ref InFlightCount),
          PendingRequests.Count,
          PendingStreams.Count,
          ActiveRequests.Count,
          OutgoingStreams.Count,
          Volatile.Read(ref QueuedHandlerFrames),
          Volatile.Read(ref ActiveHandlerWorkers),
          Volatile.Read(ref ActiveWriters),
          IncomingChunks.Count,
          Interlocked.Read(ref IncomingChunkBytes),
          lanes,
          MemoryBudget.InUse,
          Volatile.Read(ref Writer.BytesWritten),
          Volatile.Read(ref Reader.BytesRead));
    }
  }
}
