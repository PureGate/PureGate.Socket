using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// The write path.
//
// Writers encode their frame, under a short lock, into an in-memory batch (SocketWriteBuffer). One flusher
// at a time writes the whole batch to the socket in 64 KB writes, outside the lock. A writer that finds
// a flush already running only appends its frame and returns; its bytes go out with the next batch. So
// writers never wait for each other's socket writes, and a busy connection sends many frames per system
// call. (The previous design held one lock across encoding and the socket write, which serialized every
// frame behind a system call and capped throughput on a shared connection.)
//
// - Payloads: a frame is encoded synchronously, so its payload is never read after its write call
//   returns. A caller may return a pooled payload as soon as the call completes.
// - Flushing: the writer that starts a flush writes the first batch itself. If more frames arrived
//   meanwhile, the rest is written from the thread pool, so no caller keeps writing on behalf of others.
// - Backpressure: when the bytes encoded but not yet written reach WriteHighWaterBytes, writers wait (within
//   their own token) until the flusher has brought them down to half of that.
// - Large messages are sent as chunks, each encoded like any frame. Other frames may be sent between the
//   chunks, so one large message never holds up everything else on the connection.
// - Cancellation: a caller's token cancels waiting for room only. A frame is encoded whole or not at all,
//   so the stream never carries a partial frame. A chunked message whose caller gives up between chunks is
//   left incomplete; the remote discards it when it expires.
// - Failure: a socket write that fails closes the connection, which fails every outstanding request.
public sealed partial class SocketPeer
{
  readonly Lock WriteSync = new();
  readonly SocketWriteBuffer Pending = new();
  // The batch the flusher is writing; only the flusher (one at a time) touches it.
  readonly SocketWriteBatch FlushBatch = new();
  bool Flushing;
  bool WritesClosed;
  long UnflushedBytes;
  TaskCompletionSource RoomSignal;
  TaskCompletionSource IdleSignal;
  FlushWorkItem FlushWork;

  long WriteHighWaterBytes => Math.Max(1024 * 1024, 2L * Options.MaxFrameSizeBytes);

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask WriteFrameAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteFrameAsync(frame, payloadOverride: default, cancellationToken);
  }

  // Encodes and sends one frame. The payload is payloadOverride when non-empty, otherwise the frame's own.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask WriteFrameAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    var room = WaitForRoomAsync(cancellationToken);
    if (!room.IsCompletedSuccessfully)
      await room;

    OversizedFrame oversized;
    bool flush;
    lock (WriteSync)
    {
      ThrowIfWritesClosed();
      var before = Pending.Length;
      oversized = Writer.Encode(frame, payloadOverride);
      if (oversized is null)
        frame.WriteStarted = true;
      flush = CommitLocked(before);
    }

    if (oversized is not null)
    {
      await WriteChunksAsync(frame, oversized, cancellationToken);
      return;
    }

    ObserveFrameSent(frame, payloadOverride.IsEmpty ? frame.PayloadLength : payloadOverride.Length);
    if (flush)
      await FlushAsync(inline: true, cancellationToken);
  }

  // Control frames (credits, pongs, cancels, rejections) are tiny and must never make their sender wait:
  // they skip backpressure, and a flush they start runs on the thread pool. Safe to call from the read loop.
  void SendControl(SocketFrame frame)
  {
    bool flush;
    lock (WriteSync)
    {
      if (WritesClosed || Lifetime.IsCancellationRequested)
        return;

      var before = Pending.Length;
      if (Writer.Encode(frame, default) is not null)
        return;
      flush = CommitLocked(before);
    }

    ObserveFrameSent(frame, frame.PayloadLength);
    if (flush)
      QueueBackgroundFlush();
  }

  // Sends a frame too large for one wire frame as consecutive chunks of its encoded body (the header,
  // already encoded, followed by the payload). Chunks are equal-sized except the last.
  async ValueTask WriteChunksAsync(SocketFrame frame, OversizedFrame oversized, CancellationToken cancellationToken)
  {
    var header = oversized.Header;
    var payload = oversized.Payload;
    var total = oversized.TotalLength;
    var chunkSize = Options.MaxChunkPayloadBytes;
    var chunkCount = (int)(((long)total + chunkSize - 1) / chunkSize);
    for (var index = 0; index < chunkCount; index++)
    {
      var room = WaitForRoomAsync(cancellationToken);
      if (!room.IsCompletedSuccessfully)
        await room;

      var start = index * chunkSize;
      var end = (int)Math.Min(total, (long)start + chunkSize);
      bool flush;
      lock (WriteSync)
      {
        ThrowIfWritesClosed();
        var headerPart = start < header.Length
            ? header.AsSpan(start, Math.Min(end, header.Length) - start)
            : ReadOnlySpan<byte>.Empty;
        var payloadStart = Math.Max(0, start - header.Length);
        var payloadEnd = Math.Max(0, end - header.Length);
        var before = Pending.Length;
        Writer.EncodeChunk(
            oversized.MessageId,
            oversized.Kind,
            index,
            chunkCount,
            total,
            headerPart,
            payload.Span.Slice(payloadStart, payloadEnd - payloadStart));
        frame.WriteStarted = true;
        flush = CommitLocked(before);
      }

      ObserveFrameSent(SocketMessageKind.Chunk, end - start);
      if (flush)
        await FlushAsync(inline: true, cancellationToken);
    }
  }

  // Under WriteSync, after encoding: counts the new bytes, and returns true when the caller must start the
  // flusher because none is running.
  bool CommitLocked(long lengthBefore)
  {
    var added = Pending.Length - lengthBefore;
    if (added <= 0)
      return false;

    UnflushedBytes += added;
    if (Flushing)
      return false;

    Flushing = true;
    return true;
  }

  void ThrowIfWritesClosed()
  {
    if (WritesClosed || Lifetime.IsCancellationRequested)
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection closed.");
  }

  ValueTask WaitForRoomAsync(CancellationToken cancellationToken)
  {
    return Interlocked.Read(ref UnflushedBytes) < WriteHighWaterBytes
        ? ValueTask.CompletedTask
        : WaitForRoomSlowAsync(cancellationToken);
  }

  async ValueTask WaitForRoomSlowAsync(CancellationToken cancellationToken)
  {
    while (true)
    {
      Task wait;
      lock (WriteSync)
      {
        ThrowIfWritesClosed();
        if (UnflushedBytes < WriteHighWaterBytes)
          return;

        RoomSignal ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        wait = RoomSignal.Task;
      }

      await wait.WaitAsync(cancellationToken);
    }
  }

  // Writes batches until none is left, then stops being the flusher. inline: called by the writer that
  // started the flush, which writes one batch and hands any further batches to the thread pool. It waits
  // for its socket write only as long as cancellationToken allows (a remote that stops reading must not
  // keep it waiting past its own timeout); when the token fires, the write and the flushing after it carry
  // on in the background, and the caller gets its cancellation.
  async ValueTask FlushAsync(bool inline, CancellationToken cancellationToken = default)
  {
    var written = 0;
    while (true)
    {
      TaskCompletionSource idle = null;
      var handOff = false;
      var take = false;
      lock (WriteSync)
      {
        if (Pending.Length == 0 || WritesClosed)
        {
          // Nothing left to send: an idle connection keeps no write buffers. The next write rents again.
          Flushing = false;
          idle = IdleSignal;
          IdleSignal = null;
          Pending.Release();
        }
        else if (inline && written > 0)
        {
          // Stays the flusher, on the thread pool.
          handOff = true;
        }
        else
        {
          Pending.MoveTo(FlushBatch);
          take = true;
        }
      }

      if (!take)
      {
        if (handOff)
          QueueBackgroundFlush();
        idle?.TrySetResult();
        return;
      }

      var write = FlushBatch.WriteToAsync(Stream, Lifetime.Token);
      if (!write.IsCompletedSuccessfully)
      {
        if (inline && cancellationToken.CanBeCanceled)
        {
          var pending = write.AsTask();
          try
          {
            await pending.WaitAsync(cancellationToken);
          }
          catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
          {
            _ = FinishFlushInBackgroundAsync(pending);
            throw;
          }
          catch (Exception ex)
          {
            WriteFailed(ex, inline);
            return;
          }
        }
        else
        {
          try
          {
            await write;
          }
          catch (Exception ex)
          {
            WriteFailed(ex, inline);
            return;
          }
        }
      }

      written++;
      BatchWritten();
    }
  }

  // The rest of a flush whose writer stopped waiting: finish its socket write, then keep flushing.
  async Task FinishFlushInBackgroundAsync(Task write)
  {
    try
    {
      await write;
    }
    catch (Exception ex)
    {
      WriteFailed(ex, inline: false);
      return;
    }

    BatchWritten();
    await BackgroundFlushAsync();
  }

  // A socket write failed: the connection cannot be written any more, and closing it fails everything
  // outstanding. An inline writer gets the failure as connection_closed (or its cancellation).
  void WriteFailed(Exception exception, bool inline)
  {
    FlushBatch.Release();
    var lost = IsTransportFailure(exception) ? ConnectionLost(exception) : null;
    if (lost is null && exception is not OperationCanceledException)
      _ = CloseAsync(exception).AsTask();
    if (!inline)
      return;
    if (lost is not null)
      throw lost;
    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(exception);
  }

  // Bookkeeping after a batch reached the socket: counts it as flushed, returns its segments to the pool
  // and wakes writers waiting for room.
  void BatchWritten()
  {
    TaskCompletionSource room = null;
    lock (WriteSync)
    {
      // CloseWrites already reset the count.
      if (!WritesClosed)
        UnflushedBytes -= FlushBatch.Length;
      if (RoomSignal is not null && UnflushedBytes <= WriteHighWaterBytes / 2)
      {
        room = RoomSignal;
        RoomSignal = null;
      }
    }

    FlushBatch.Release();
    room?.TrySetResult();
  }

  void QueueBackgroundFlush()
  {
    ThreadPool.UnsafeQueueUserWorkItem(FlushWork ??= new FlushWorkItem(this), preferLocal: false);
  }

  async Task BackgroundFlushAsync()
  {
    try
    {
      await FlushAsync(inline: false);
    }
    catch
    {
      // FlushAsync closes the connection on failure; nothing is waiting for this task.
    }
  }

  sealed class FlushWorkItem(SocketPeer peer) : IThreadPoolWorkItem
  {
    public void Execute()
    {
      _ = peer.BackgroundFlushAsync();
    }
  }

  // Completes once everything encoded so far has been written (or the connection closed).
  async ValueTask WaitForWritesAsync(CancellationToken cancellationToken)
  {
    Task wait;
    lock (WriteSync)
    {
      if (!Flushing || WritesClosed)
        return;

      IdleSignal ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      wait = IdleSignal.Task;
    }

    await wait.WaitAsync(cancellationToken);
  }

  // Called when the connection closes: no frame is encoded after this, waiting writers wake up (and see
  // the connection closed), and the batch buffers go back to the pool.
  void CloseWrites()
  {
    TaskCompletionSource room;
    TaskCompletionSource idle;
    lock (WriteSync)
    {
      WritesClosed = true;
      room = RoomSignal;
      idle = IdleSignal;
      RoomSignal = null;
      IdleSignal = null;
      // A flush in progress still owns its batch and releases the rest when it stops.
      if (!Flushing)
        Pending.Release();
      UnflushedBytes = 0;
    }

    room?.TrySetResult();
    idle?.TrySetResult();
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
}
