using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  static readonly long ChunkPruneIntervalTicks = TimeSpan.FromSeconds(1).Ticks;

  // Only the read loop touches the chunk state below, so it needs no synchronization.
  long IncomingChunkBytes;
  long LastChunkPruneTicks = DateTimeOffset.UtcNow.UtcTicks;

  // A sender writes the chunks of one message back to back, in order, with equal sizes except the last
  // (see WriteChunkedFrameAsync). Anything else is a protocol violation and closes the connection.
  // Memory is committed as bytes arrive, never from the sender's declared sizes.
  SocketFrame ReassembleChunk(SocketFrame frame)
  {
    ValidateChunk(frame);
    var key = new ChunkKey(frame.OriginalKind, frame.MessageId);
    if (!IncomingChunks.TryGetValue(key, out var incoming))
    {
      if (frame.ChunkIndex != 0)
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunked message did not start with its first chunk.");
      if (IncomingChunks.Count >= Options.MaxChunkedMessages)
        throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Too many chunked messages are in progress.");

      incoming = new IncomingChunkedFrame(frame);
      IncomingChunks[key] = incoming;
    }

    if (IncomingChunkBytes + frame.PayloadLength > Options.MaxChunkedBytesInProgress)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Chunked messages in progress exceed the configured memory budget.");

    if (!incoming.TryAppend(frame))
    {
      RemoveIncomingChunks(key, incoming);
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunk is out of order or inconsistent with its message.");
    }

    IncomingChunkBytes += frame.PayloadLength;
    if (!incoming.Completed)
      return null;

    RemoveIncomingChunks(key, incoming);
    return incoming.ToFrame();
  }

  void RemoveIncomingChunks(ChunkKey key, IncomingChunkedFrame incoming)
  {
    if (IncomingChunks.TryRemove(key, out _))
      IncomingChunkBytes -= incoming.ReceivedBytes;
  }

  void PruneExpiredChunks()
  {
    var now = DateTimeOffset.UtcNow;
    if (IncomingChunks.IsEmpty || now.UtcTicks - LastChunkPruneTicks < ChunkPruneIntervalTicks)
      return;

    LastChunkPruneTicks = now.UtcTicks;
    foreach (var item in IncomingChunks)
    {
      if (now - item.Value.CreatedAt > Options.ChunkTimeout)
        RemoveIncomingChunks(item.Key, item.Value);
    }
  }

  void PruneExpiredIdempotentRequests()
  {
    Options.IdempotencyCache.Prune(Options.IdempotencyTtl, Options.MaxIdempotencyEntries);
  }

  void ValidateChunk(SocketFrame frame)
  {
    if (frame.OriginalKind is not (SocketMessageKind.Request or SocketMessageKind.Response or SocketMessageKind.Event or SocketMessageKind.StreamItem or SocketMessageKind.FastRequest or SocketMessageKind.FastResponse))
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

      Buffer = new byte[Math.Min(TotalLength, ChunkSize * 2L)];
      CreatedAt = DateTimeOffset.UtcNow;
    }

    public DateTimeOffset CreatedAt { get; }
    public int ReceivedBytes { get; private set; }
    public bool Completed => NextIndex == ChunkCount;

    public bool TryAppend(SocketFrame frame)
    {
      if (frame.ChunkIndex != NextIndex
          || frame.ChunkCount != ChunkCount
          || frame.TotalPayloadLength != TotalLength
          || frame.OriginalKind != FirstFrame.OriginalKind
          || frame.OperationId != FirstFrame.OperationId
          || frame.Service != FirstFrame.Service
          || frame.Operation != FirstFrame.Operation
          || frame.IdempotencyKey != FirstFrame.IdempotencyKey)
      {
        return false;
      }

      var expectedLength = NextIndex == ChunkCount - 1
          ? TotalLength - (long)ChunkSize * (ChunkCount - 1)
          : ChunkSize;
      if (frame.PayloadLength != expectedLength)
        return false;

      EnsureCapacity(ReceivedBytes + frame.PayloadLength);
      frame.PayloadMemory.Span.CopyTo(Buffer.AsSpan(ReceivedBytes));
      ReceivedBytes += frame.PayloadLength;
      NextIndex++;
      return true;
    }

    void EnsureCapacity(int required)
    {
      if (required <= Buffer.Length)
        return;

      var capacity = (int)Math.Min(TotalLength, Math.Max(required, Buffer.Length * 2L));
      Array.Resize(ref Buffer, capacity);
    }

    public SocketFrame ToFrame()
    {
      var frame = new SocketFrame
      {
        Version = FirstFrame.Version,
        MessageId = FirstFrame.MessageId,
        Kind = FirstFrame.OriginalKind,
        OperationId = FirstFrame.OperationId,
        Service = FirstFrame.Service,
        Operation = FirstFrame.Operation,
        IdempotencyKey = FirstFrame.IdempotencyKey,
        Flags = FirstFrame.Flags,
        DeadlineMs = FirstFrame.DeadlineMs,
        Ok = FirstFrame.Ok,
        ErrorCode = FirstFrame.ErrorCode,
        ErrorMessage = FirstFrame.ErrorMessage
      };
      frame.SetPayload(SocketPayload.Wrap(Buffer, 0, TotalLength));
      Buffer = null;
      return frame;
    }
  }
}
