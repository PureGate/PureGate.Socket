using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  SocketFrame ReassembleChunk(SocketFrame frame)
  {
    ValidateChunk(frame);
    var key = new ChunkKey(frame.OriginalKind, frame.MessageId);
    var incoming = IncomingChunks.GetOrAdd(key, _ =>
    {
      if (IncomingChunks.Count >= Options.MaxChunkedMessages)
        throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Too many chunked messages are in progress.");

      return new IncomingChunkedFrame(frame);
    });

    if (!incoming.TryAdd(frame))
    {
      IncomingChunks.TryRemove(key, out _);
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Duplicate chunk received.");
    }

    if (!incoming.Completed)
      return null;

    IncomingChunks.TryRemove(key, out _);
    return incoming.ToFrame();
  }

  void PruneExpiredChunks()
  {
    var now = DateTimeOffset.UtcNow;
    foreach (var item in IncomingChunks)
    {
      if (now - item.Value.CreatedAt > Options.ChunkTimeout)
        IncomingChunks.TryRemove(item.Key, out _);
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
    if (frame.TotalPayloadLength < 0 || frame.TotalPayloadLength > Options.MaxDecompressedPayloadSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Chunked payload exceeds the configured maximum size.");
  }
  readonly record struct ChunkKey(SocketMessageKind Kind, long MessageId);

  sealed class IncomingChunkedFrame
  {
    readonly byte[][] Chunks;
    readonly SocketFrame FirstFrame;
    int ReceivedCount;
    int ReceivedBytes;

    public IncomingChunkedFrame(SocketFrame firstFrame)
    {
      FirstFrame = firstFrame;
      Chunks = new byte[firstFrame.ChunkCount][];
      CreatedAt = DateTimeOffset.UtcNow;
    }

    public DateTimeOffset CreatedAt { get; }
    public bool Completed => ReceivedCount == Chunks.Length;

    public bool TryAdd(SocketFrame frame)
    {
      if (frame.ChunkCount != Chunks.Length
          || frame.TotalPayloadLength != FirstFrame.TotalPayloadLength
          || frame.OriginalKind != FirstFrame.OriginalKind
          || frame.OperationId != FirstFrame.OperationId
          || frame.Service != FirstFrame.Service
          || frame.Operation != FirstFrame.Operation
          || frame.IdempotencyKey != FirstFrame.IdempotencyKey)
      {
        return false;
      }

      if (Chunks[frame.ChunkIndex] is not null)
        return false;

      var payload = frame.PayloadMemory.ToArray();
      Chunks[frame.ChunkIndex] = payload;
      ReceivedBytes += payload.Length;
      ReceivedCount++;
      return ReceivedBytes <= frame.TotalPayloadLength;
    }

    public SocketFrame ToFrame()
    {
      if (ReceivedBytes != FirstFrame.TotalPayloadLength)
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunked payload length does not match.");

      var payload = new byte[ReceivedBytes];
      var offset = 0;
      foreach (var chunk in Chunks)
      {
        if (chunk is null)
          throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Chunked payload is incomplete.");

        Buffer.BlockCopy(chunk, 0, payload, offset, chunk.Length);
        offset += chunk.Length;
      }

      return new SocketFrame
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
        ErrorMessage = FirstFrame.ErrorMessage,
        Payload = payload
      };
    }
  }
}
