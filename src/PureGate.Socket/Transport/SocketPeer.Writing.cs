using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  async ValueTask WriteFrameAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteFrameAsync(frame, payloadOverride: default, cancellationToken);
  }

  async ValueTask WriteFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    await WriteLock.WaitAsync(cancellationToken);
    try
    {
      try
      {
        if (payloadOverride.IsEmpty)
          await Writer.WriteAsync(frame, cancellationToken);
        else
          await Writer.WriteAsync(frame, payloadOverride, cancellationToken);
        ObserveFrameSent(frame);
      }
      catch (SocketProtocolException ex)
          when (ex.ErrorCode == SocketProtocolError.PayloadTooLarge
              && frame.Kind != SocketMessageKind.Chunk
              && (frame.PayloadLength > 0 || payloadOverride.Length > 0))
      {
        await WriteChunkedFrameAsync(
            frame,
            payloadOverride.IsEmpty ? frame.PayloadMemory : payloadOverride,
            cancellationToken);
      }
    }
    finally
    {
      WriteLock.Release();
    }
  }

  async ValueTask WriteChunkedFrameAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteChunkedFrameAsync(frame, frame.PayloadMemory, cancellationToken);
  }

  async ValueTask WriteChunkedFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payload,
      CancellationToken cancellationToken)
  {
    var chunkSize = Math.Min(Options.MaxChunkPayloadBytes, payload.Length);
    if (chunkSize <= 0)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame exceeds the configured maximum size.");

    var chunkCount = (payload.Length + chunkSize - 1) / chunkSize;
    for (var index = 0; index < chunkCount; index++)
    {
      var offset = index * chunkSize;
      var count = Math.Min(chunkSize, payload.Length - offset);
      var chunk = new SocketFrame
      {
        Version = frame.Version,
        MessageId = frame.MessageId,
        Kind = SocketMessageKind.Chunk,
        OriginalKind = frame.Kind,
        OperationId = frame.OperationId,
        Service = frame.Service,
        Operation = frame.Operation,
        IdempotencyKey = frame.IdempotencyKey,
        Flags = frame.Flags,
        DeadlineMs = frame.DeadlineMs,
        ChunkIndex = index,
        ChunkCount = chunkCount,
        TotalPayloadLength = payload.Length,
        Ok = frame.Ok,
        ErrorCode = frame.ErrorCode,
        ErrorMessage = frame.ErrorMessage
      };

      await Writer.WriteAsync(chunk, payload.Slice(offset, count), cancellationToken);
      ObserveFrameSent(chunk);
    }
  }
}
