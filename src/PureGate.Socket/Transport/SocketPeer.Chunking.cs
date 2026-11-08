using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  static readonly long ChunkPruneIntervalTicks = TimeSpan.FromSeconds(1).Ticks;

  // Only the read loop touches the chunk state below, so it needs no synchronization.
  // IncomingChunkBytes is the payload received for messages still in progress; it is what is reserved
  // from the connection's memory budget.
  long IncomingChunkBytes;
  long LastChunkPruneTicks = DateTimeOffset.UtcNow.UtcTicks;

  // A sender writes the chunks of one message in order, with equal sizes except the last (see
  // WriteChunksAsync); other frames may come between them. A chunk that breaks those rules is a protocol
  // violation and closes the connection. Memory is committed as bytes arrive, never from the sender's
  // declared sizes. Chunk payloads are pooled buffers (see SocketFrameReader) kept as they are until the
  // last chunk arrives; the message is then copied once into an array of its exact size.
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
    if (!MemoryBudget.TryReserve(frame.PayloadLength))
    {
      RemoveIncomingChunks(key, incoming);
      RejectOverBudget(frame.OriginalKind, frame.MessageId, creditTaken: true);
      return null;
    }

    IncomingChunkBytes += frame.PayloadLength;
    incoming.Append(frame);
    if (!incoming.Completed)
      return null;

    // The reassembled payload is exactly the bytes received, so the reservation moves to the frame.
    IncomingChunks.TryRemove(key, out _);
    var reserved = (long)incoming.ReceivedBytes;
    IncomingChunkBytes -= reserved;
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
    finally
    {
      incoming.Release();
    }
  }

  void RemoveIncomingChunks(ChunkKey key, IncomingChunkedFrame incoming)
  {
    if (!IncomingChunks.TryRemove(key, out _))
      return;

    IncomingChunkBytes -= incoming.ReceivedBytes;
    MemoryBudget.Release(incoming.ReceivedBytes);
    incoming.Release();
  }

  // Returns the pooled buffers of every message still being reassembled. Called when the connection closes.
  void ReleaseIncomingChunks()
  {
    foreach (var item in IncomingChunks)
      RemoveIncomingChunks(item.Key, item.Value);
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
      if (now - item.Value.LastActivity <= Options.ChunkTimeout)
        continue;

      RemoveIncomingChunks(item.Key, item.Value);
      // Its sender spent a message credit that no handler will return (it gave up part way, typically
      // because its call was cancelled), so it is returned here.
      if (item.Key.Kind is SocketMessageKind.Request or SocketMessageKind.Event or SocketMessageKind.StreamRequest)
        ReturnDroppedMessageCredit();
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
    readonly List<SocketPayload> Parts = [];
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

      LastActivity = DateTimeOffset.UtcNow;
    }

    public DateTimeOffset LastActivity { get; private set; }
    public bool Completed => NextIndex == ChunkCount;
    public int ReceivedBytes { get; private set; }

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

    // Keeps the chunk's payload (a pooled buffer) as it is: no copy until the message is complete.
    // The read loop appends; a closing connection may release at the same moment, hence the lock. A chunk
    // that arrives after the release is returned to the pool at once.
    public void Append(SocketFrame frame)
    {
      var payload = frame.TakePayload();
      lock (Parts)
      {
        if (Released)
        {
          payload.Dispose();
          return;
        }

        Parts.Add(payload);
      }

      ReceivedBytes += payload.Length;
      NextIndex++;
      LastActivity = DateTimeOffset.UtcNow;
    }

    bool Released;

    public void Release()
    {
      lock (Parts)
      {
        Released = true;
        foreach (var part in Parts)
          part.Dispose();
        Parts.Clear();
      }
    }

    // The chunks together are the encoded body of the original frame (header and payload), so it is
    // decoded like any other frame. They are copied once into an array of the exact size, and the payload
    // is exposed in place there, since handlers may keep it.
    public SocketFrame ToFrame(SocketFrameReader reader)
    {
      var buffer = GC.AllocateUninitializedArray<byte>(TotalLength);
      var offset = 0;
      lock (Parts)
      {
        if (Released)
          throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed.");
        foreach (var part in Parts)
        {
          part.Span.CopyTo(buffer.AsSpan(offset));
          offset += part.Length;
        }
      }

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
