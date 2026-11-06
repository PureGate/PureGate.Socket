using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  async ValueTask WriteFrameAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteFrameAsync(frame, payloadOverride: default, cancellationToken);
  }

  // Cancellation contract. The caller's token only cancels waiting for the write lock. Once a frame has
  // the lock, every wire frame is written whole with the connection's own token, so the stream is never
  // left with a partial frame. Two reasons:
  // - A frame cancelled half way would leave a partial frame on the wire and corrupt the stream.
  // - The pipe writer is shared by every caller. A token handed to it can trip the next, unrelated write
  //   (one request's timeout could otherwise fail a different caller's write).
  //
  // A caller whose token fires while its frame is being flushed stops waiting and the flush finishes in
  // the background. The payload is usually the caller's own array (SocketFrame.Payload wraps it without
  // copying), so the background write must never read it after the caller has returned:
  // - A single frame copies the payload into the pipe synchronously, before its first await, so by the
  //   time the caller can stop waiting the payload is no longer read.
  // - A chunked frame reads the next slice after each flush. A PayloadLease guards those reads: the
  //   caller revokes it before returning, and the writer checks it before copying each chunk, then stops.
  //   The remote discards the partial chunked message when it expires.
  // A payloadOverride buffer is always awaited to the end; it goes back to its owner when this returns.
  async ValueTask WriteFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    await WriteLock.WaitAsync(cancellationToken);
    var abandonable = cancellationToken.CanBeCanceled && payloadOverride.IsEmpty;
    var lease = abandonable && frame.PayloadLength > Options.MaxChunkPayloadBytes ? new PayloadLease() : null;
    var write = WriteLockedFrameAsync(frame, payloadOverride, lease);
    if (write.IsCompleted || !abandonable)
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
      lease?.Revoke();
      _ = pending.ContinueWith(
          static task => _ = task.Exception,
          CancellationToken.None,
          TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
          TaskScheduler.Default);
      throw;
    }
  }

  // Runs with WriteLock held and releases it when the frame is fully written (or a chunked write stops
  // because its lease was revoked).
  async ValueTask WriteLockedFrameAsync(SocketFrame frame, ReadOnlyMemory<byte> payloadOverride, PayloadLease lease)
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
            lease,
            cancellationToken);
      }
    }
    finally
    {
      WriteLock.Release();
    }
  }

  async ValueTask WriteChunkedFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payload,
      PayloadLease lease,
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

      // The copy into the pipe happens synchronously inside WriteAsync, before it first awaits, so the
      // payload is only read while the lease is held.
      if (lease is not null && !lease.TryAcquire())
        throw new OperationCanceledException("The caller cancelled; the rest of the chunked message is not sent.");

      ValueTask write;
      try
      {
        write = Writer.WriteAsync(chunk, payload.Slice(offset, count), cancellationToken);
      }
      finally
      {
        lease?.Release();
      }

      await write;
      ObserveFrameSent(chunk);
    }
  }

  // Lets a background chunked write read the caller's payload only while the caller is still waiting.
  sealed class PayloadLease
  {
    const int Free = 0;
    const int InUse = 1;
    const int Revoked = 2;
    int State;

    public bool TryAcquire()
    {
      return Interlocked.CompareExchange(ref State, InUse, Free) == Free;
    }

    public void Release()
    {
      Interlocked.CompareExchange(ref State, Free, InUse);
    }

    // Waits out a copy in progress (a few microseconds at most), then forbids further reads.
    public void Revoke()
    {
      var spin = new SpinWait();
      while (true)
      {
        var state = Interlocked.CompareExchange(ref State, Revoked, Free);
        if (state is Free or Revoked)
          return;
        spin.SpinOnce();
      }
    }
  }
}
