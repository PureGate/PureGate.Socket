using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  async ValueTask WriteFrameAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteFrameAsync(frame, payloadOverride: default, cancellationToken);
  }

  // Cancellation contract. The caller's token only cancels waiting for the write lock. Once a frame has the
  // lock it is written to completion with the connection's own token, so it either reaches the wire whole
  // or the connection closes. Two reasons:
  // - A frame cancelled half way would leave a partial frame on the wire and corrupt the stream.
  // - The pipe writer is shared by every caller. A token handed to it can trip the next, unrelated write
  //   (observed in Phase 1: one request's timeout failed a different caller's write).
  // A caller whose token fires while its frame is being written stops waiting; the write finishes in the
  // background. That is only safe when the frame owns its payload, because a payloadOverride buffer goes
  // back to its owner as soon as this method returns, so those writes are always awaited to the end.
  async ValueTask WriteFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    await WriteLock.WaitAsync(cancellationToken);
    var write = WriteLockedFrameAsync(frame, payloadOverride);
    if (write.IsCompleted || !cancellationToken.CanBeCanceled || !payloadOverride.IsEmpty)
    {
      await write;
      return;
    }

    var pending = write.AsTask();
    try
    {
      await pending.WaitAsync(cancellationToken);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      _ = pending.ContinueWith(
          static task => _ = task.Exception,
          CancellationToken.None,
          TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
          TaskScheduler.Default);
      throw;
    }
  }

  // Runs with WriteLock held and releases it when the frame is fully written.
  async ValueTask WriteLockedFrameAsync(SocketFrame frame, ReadOnlyMemory<byte> payloadOverride)
  {
    var cancellationToken = Lifetime.Token;
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
