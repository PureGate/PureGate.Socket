using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  static readonly long ChunkPruneIntervalTicks = TimeSpan.FromSeconds(1).Ticks;

  // Only the read loop touches the chunk state below, so it needs no synchronization.
  // IncomingChunkBytes is the allocated capacity of every reassembly buffer, not the bytes received; that
  // capacity is what is reserved from the connection's memory budget.
  long IncomingChunkBytes;
  long LastChunkPruneTicks = DateTimeOffset.UtcNow.UtcTicks;

  // A sender writes the chunks of one message back to back, in order, with equal sizes except the last
  // (see WriteChunkedFrameAsync). A chunk that breaks those rules is a protocol violation and closes the
  // connection. Memory is committed as bytes arrive, never from the sender's declared sizes.
  SocketFrame ReassembleChunk(SocketFrame frame)
  {
    ValidateChunk(frame);
    var key = new ChunkKey(frame.OriginalKind, frame.MessageId);
    if (!IncomingChunks.TryGetValue(key, out var incoming))
    {
      // The rest of a message that expired (ChunkTimeout without progress) or whose first chunk never
      // arrived. Its sender's request times out; dropping the chunk keeps the connection alive.
      if (frame.ChunkIndex != 0)
        return null;
      if (IncomingChunks.Count >= Options.MaxChunkedMessages)
        throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Too many chunked messages are in progress.");

      incoming = new IncomingChunkedFrame(frame);
      IncomingChunks[key] = incoming;
    }

    if (!incoming.IsNextChunk(frame))
    {
      RemoveIncomingChunks(key, incoming);
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunk is out of order or inconsistent with its message.");
    }

    // A message that does not fit is dropped and refused (its remaining chunks find no entry and are
    // dropped too), so one oversized message does not cost the connection.
    var capacity = incoming.PlanCapacity(frame.PayloadLength, MemoryBudget.Available);
    if (capacity < 0 || !MemoryBudget.TryReserve(capacity - incoming.Capacity))
    {
      RemoveIncomingChunks(key, incoming);
      RejectOverBudget(frame.OriginalKind, frame.MessageId, creditTaken: true);
      return null;
    }

    IncomingChunkBytes += capacity - incoming.Capacity;
    incoming.Append(frame, capacity);
    if (!incoming.Completed)
      return null;

    // The reassembly buffer becomes the frame's payload, so its reservation moves to the frame.
    IncomingChunks.TryRemove(key, out _);
    IncomingChunkBytes -= incoming.Capacity;
    var reserved = (long)incoming.Capacity;
    try
    {
      var whole = incoming.ToFrame(Reader);
      whole.ReservedBytes = reserved;
      return whole;
    }
    catch
    {
      MemoryBudget.Release(reserved);
      throw;
    }
  }

  void RemoveIncomingChunks(ChunkKey key, IncomingChunkedFrame incoming)
  {
    if (!IncomingChunks.TryRemove(key, out _))
      return;

    IncomingChunkBytes -= incoming.Capacity;
    MemoryBudget.Release(incoming.Capacity);
  }

  // A message expires when no chunk for it has arrived for ChunkTimeout. Measuring from the last chunk,
  // not the first, lets a large transfer take as long as it needs while it keeps making progress.
  void PruneExpiredChunks()
  {
    if (IncomingChunks.IsEmpty)
      return;

    var now = DateTimeOffset.UtcNow;
    if (now.UtcTicks - LastChunkPruneTicks < ChunkPruneIntervalTicks)
      return;

    LastChunkPruneTicks = now.UtcTicks;
    foreach (var item in IncomingChunks)
    {
      if (now - item.Value.LastActivity > Options.ChunkTimeout)
        RemoveIncomingChunks(item.Key, item.Value);
    }
  }

  void PruneExpiredIdempotentRequests()
  {
    Options.IdempotencyCache.Prune(Options.IdempotencyTtl, Options.MaxIdempotencyEntries);
  }

  void ValidateChunk(SocketFrame frame)
  {
    if (frame.OriginalKind is not (SocketMessageKind.Request or SocketMessageKind.Response or SocketMessageKind.Event or SocketMessageKind.StreamRequest or SocketMessageKind.StreamItem))
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid chunk message kind.");
    if (frame.ChunkCount <= 0 || frame.ChunkIndex < 0 || frame.ChunkIndex >= frame.ChunkCount)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid chunk index.");
    if (frame.TotalPayloadLength <= 0 || frame.PayloadLength <= 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunk carries no payload.");
    if (frame.TotalPayloadLength > Options.MaxDecompressedPayloadSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Chunked payload exceeds the configured maximum size.");
  }

  readonly record struct ChunkKey(SocketMessageKind Kind, long MessageId);

  sealed class IncomingChunkedFrame
  {
    readonly SocketFrame FirstFrame;
    readonly int ChunkSize;
    readonly int ChunkCount;
    readonly int TotalLength;
    byte[] Buffer;
    int NextIndex;

    public IncomingChunkedFrame(SocketFrame firstFrame)
    {
      FirstFrame = firstFrame;
      ChunkSize = firstFrame.PayloadLength;
      ChunkCount = firstFrame.ChunkCount;
      TotalLength = firstFrame.TotalPayloadLength;
      var expectedCount = ((long)TotalLength + ChunkSize - 1) / ChunkSize;
      if (expectedCount != ChunkCount)
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunk count does not match the payload length.");

      Buffer = [];
      LastActivity = DateTimeOffset.UtcNow;
    }

    public DateTimeOffset LastActivity { get; private set; }
    public int Capacity => Buffer?.Length ?? 0;
    public bool Completed => NextIndex == ChunkCount;
    int ReceivedBytes;

    public bool IsNextChunk(SocketFrame frame)
    {
      if (frame.ChunkIndex != NextIndex
          || frame.ChunkCount != ChunkCount
          || frame.TotalPayloadLength != TotalLength
          || frame.OriginalKind != FirstFrame.OriginalKind)
      {
        return false;
      }

      var expectedLength = NextIndex == ChunkCount - 1
          ? TotalLength - (long)ChunkSize * (ChunkCount - 1)
          : ChunkSize;
      return frame.PayloadLength == expectedLength;
    }

    // The buffer capacity needed to append `length` bytes. It doubles when the budget allows (so a large
    // message is not copied once per chunk), grows to exactly what is needed when only that fits, and
    // returns -1 when even that would exceed the budget.
    public long PlanCapacity(int length, long budgetAvailable)
    {
      long required = ReceivedBytes + length;
      if (required <= Capacity)
        return Capacity;

      var preferred = Math.Min(TotalLength, Math.Max(required, Capacity * 2L));
      if (preferred - Capacity <= budgetAvailable)
        return preferred;
      return required - Capacity <= budgetAvailable ? required : -1;
    }

    public void Append(SocketFrame frame, long capacity)
    {
      if (capacity > Capacity)
        Grow((int)capacity);

      frame.PayloadMemory.Span.CopyTo(Buffer.AsSpan(ReceivedBytes));
      ReceivedBytes += frame.PayloadLength;
      NextIndex++;
      LastActivity = DateTimeOffset.UtcNow;
    }

    // Like Array.Resize, but skips zeroing the new buffer: the received bytes are copied over, and the rest
    // is filled by later chunks before the payload is handed out (ToFrame only runs once all have arrived).
    void Grow(int capacity)
    {
      var grown = GC.AllocateUninitializedArray<byte>(capacity);
      Buffer.AsSpan(0, ReceivedBytes).CopyTo(grown);
      Buffer = grown;
    }

    // The chunks together are the encoded body of the original frame (header and payload), so it is
    // decoded like any other frame. Its payload stays in the reassembly buffer instead of being copied.
    public SocketFrame ToFrame(SocketFrameReader reader)
    {
      var buffer = Buffer;
      Buffer = null;
      var frame = reader.DecodeBody(new System.Buffers.ReadOnlySequence<byte>(buffer, 0, TotalLength), buffer);
      if (frame.Kind != FirstFrame.OriginalKind || frame.MessageId != FirstFrame.MessageId)
      {
        frame.ReleasePayload();
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Reassembled frame does not match its chunks.");
      }

      return frame;
    }
  }
}
