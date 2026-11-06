using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  // Writers that have asked for WriteLock and not yet left: the holder plus everyone queued behind it.
  int ActiveWriters;
  // 1 when encoded bytes may be sitting in the pipe unflushed.
  int FlushPending;

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask WriteFrameAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteFrameAsync(frame, payloadOverride: default, cancellationToken);
  }

  // Flush coalescing. A writer encodes its frame while holding WriteLock and then flushes only if no other
  // writer is queued for the lock. If one is, the bytes stay buffered and that writer sends them together
  // with its own frame in a single flush, so a busy connection makes one system call for many frames.
  // An idle connection flushes every frame immediately, exactly as before. If a queued writer gives up
  // (its token fires while waiting) and it was the last one, it schedules the flush itself, so buffered
  // bytes are never left behind.
  //
  // Cancellation contract. The caller's token only cancels waiting for the write lock. Once a frame has
  // the lock, every wire frame is encoded whole and flushed with the connection's own token, so the stream
  // is never left with a partial frame. Two reasons:
  // - A frame cancelled half way would leave a partial frame on the wire and corrupt the stream.
  // - The pipe writer is shared by every caller. A token handed to it can trip the next, unrelated write
  //   (one request's timeout could otherwise fail a different caller's write).
  //
  // A caller whose token fires while its frame is being flushed stops waiting and the flush finishes in
  // the background. The payload is usually the caller's own array (SocketFrame.Payload wraps it without
  // copying), so the background write must never read it after the caller has returned:
  // - A single frame is encoded into the pipe synchronously, before its first await, so by the time the
  //   caller can stop waiting the payload is no longer read.
  // - A chunked frame encodes the next slice after each flush. A PayloadLease guards those reads: the
  //   caller revokes it before returning, and the writer checks it before encoding each chunk, then stops.
  //   The remote discards the partial chunked message when it expires.
  // A payloadOverride buffer is always awaited to the end; it goes back to its owner when this returns.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask WriteFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    Interlocked.Increment(ref ActiveWriters);
    try
    {
      await WriteLock.WaitAsync(cancellationToken);
    }
    catch
    {
      LeaveWithoutWriting();
      throw;
    }

    frame.WriteStarted = true;
    var abandonable = cancellationToken.CanBeCanceled && payloadOverride.IsEmpty;
    var lease = abandonable && MayBeChunked(frame) ? new PayloadLease() : null;
    var write = WriteLockedFrameAsync(frame, payloadOverride, lease);
    if (write.IsCompleted || !abandonable)
    {
      try
      {
        await write;
      }
      catch (Exception ex) when (IsTransportFailure(ex))
      {
        throw ConnectionLost(ex);
      }

      return;
    }

    var pending = write.AsTask();
    try
    {
      await pending.WaitAsync(cancellationToken);
    }
    catch (Exception ex) when (IsTransportFailure(ex))
    {
      throw ConnectionLost(ex);
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

  // Conservative: true for any frame whose encoded body could exceed one wire frame (a chunk is at most a
  // quarter of the frame limit, and a UTF-8 character at most three bytes).
  bool MayBeChunked(SocketFrame frame)
  {
    var text = (long)(frame.Service?.Length ?? 0) + (frame.Operation?.Length ?? 0) + (frame.IdempotencyKey?.Length ?? 0)
        + (frame.ErrorCode?.Length ?? 0) + (frame.ErrorMessage?.Length ?? 0);
    return frame.PayloadLength + 3 * text + 64 > Options.MaxChunkPayloadBytes;
  }

  // The socket failed under a writer (reset, disposed by a concurrent close, or another I/O error). The
  // connection is unusable: close it and report the same connection_closed error every other path uses,
  // so callers never see raw transport exception types.
  static bool IsTransportFailure(Exception exception)
  {
    return exception is IOException or ObjectDisposedException or System.Net.Sockets.SocketException;
  }

  SocketProtocolException ConnectionLost(Exception exception)
  {
    // CloseAsync cancels Lifetime before its first await, so the connection is marked closed on return.
    _ = CloseAsync(exception).AsTask();
    return new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed.");
  }

  // Runs with WriteLock held; FinishWriteAsync releases it.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask WriteLockedFrameAsync(SocketFrame frame, ReadOnlyMemory<byte> payloadOverride, PayloadLease lease)
  {
    try
    {
      var oversized = Writer.Encode(frame, payloadOverride);
      if (oversized is null)
        ObserveFrameSent(frame, payloadOverride.IsEmpty ? frame.PayloadLength : payloadOverride.Length);
      else
        await WriteChunkedFrameAsync(oversized, lease, Lifetime.Token);
    }
    catch
    {
      try
      {
        await FinishWriteAsync();
      }
      catch
      {
      }

      throw;
    }

    await FinishWriteAsync();
  }

  // Leaves the writer queue while holding WriteLock, flushing if nobody is queued behind, then releases
  // the lock. FlushPending is raised before leaving so a queued writer that gives up afterwards sees it.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask FinishWriteAsync()
  {
    try
    {
      Volatile.Write(ref FlushPending, 1);
      if (Interlocked.Decrement(ref ActiveWriters) == 0)
      {
        Volatile.Write(ref FlushPending, 0);
        await Writer.FlushAsync(Lifetime.Token);
      }
    }
    finally
    {
      WriteLock.Release();
    }
  }

  // A queued writer that gave up before getting the lock. If it was the last writer and bytes are still
  // buffered, nobody else will flush them, so it does (in the background, so the caller is not delayed).
  void LeaveWithoutWriting()
  {
    if (Interlocked.Decrement(ref ActiveWriters) == 0 && Volatile.Read(ref FlushPending) != 0)
      _ = FlushPendingAsync();
  }

  async Task FlushPendingAsync()
  {
    Interlocked.Increment(ref ActiveWriters);
    try
    {
      await WriteLock.WaitAsync(Lifetime.Token);
    }
    catch
    {
      // The connection is closing; buffered bytes no longer matter.
      Interlocked.Decrement(ref ActiveWriters);
      return;
    }

    try
    {
      await FinishWriteAsync();
    }
    catch
    {
    }
  }

  // Sends a frame too large for one wire frame as consecutive chunks of its encoded body (the header,
  // already encoded, followed by the payload). Chunks are equal-sized except the last.
  async ValueTask WriteChunkedFrameAsync(OversizedFrame oversized, PayloadLease lease, CancellationToken cancellationToken)
  {
    var header = oversized.Header;
    var payload = oversized.Payload;
    var total = oversized.TotalLength;
    var chunkSize = Options.MaxChunkPayloadBytes;
    var chunkCount = (int)(((long)total + chunkSize - 1) / chunkSize);
    for (var index = 0; index < chunkCount; index++)
    {
      var start = index * chunkSize;
      var end = (int)Math.Min(total, (long)start + chunkSize);
      var headerPart = start < header.Length
          ? header.AsSpan(start, Math.Min(end, header.Length) - start)
          : ReadOnlySpan<byte>.Empty;
      var payloadStart = Math.Max(0, start - header.Length);
      var payloadEnd = Math.Max(0, end - header.Length);

      // Encoding is synchronous, so the payload is only read while the lease is held.
      var touchesPayload = payloadEnd > payloadStart;
      if (touchesPayload && lease is not null && !lease.TryAcquire())
        throw new OperationCanceledException("The caller cancelled; the rest of the chunked message is not sent.");

      try
      {
        Writer.EncodeChunk(
            oversized.MessageId,
            oversized.Kind,
            index,
            chunkCount,
            total,
            headerPart,
            payload.Span.Slice(payloadStart, payloadEnd - payloadStart));
      }
      finally
      {
        if (touchesPayload)
          lease?.Release();
      }

      ObserveFrameSent(SocketMessageKind.Chunk, end - start);

      // Every chunk but the last is flushed here, so a large message never sits whole in the pipe. The
      // last one is flushed (or coalesced) by FinishWriteAsync like any other frame.
      if (index < chunkCount - 1)
        await Writer.FlushAsync(cancellationToken);
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

    // Waits out an encode in progress (a few microseconds at most), then forbids further reads.
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
