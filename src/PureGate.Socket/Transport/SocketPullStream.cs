using System.Threading.Channels;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// The consumer end of a pull stream, with byte-level flow control.
//
// The producer may send only as many bytes as this side has granted: the initial window, plus every
// grant sent since. The read loop therefore accepts each chunk immediately (never waiting for the
// application to read), and buffered data is bounded by the window. A producer that sends past its grant
// is breaking the protocol and the connection closes.
//
// Grants follow the application's reads: once it has consumed a quarter window since the last grant, that
// amount is granted back. Granting early keeps three quarters of the window in flight, so the producer's
// refill reaches the consumer before it runs dry; a steady reader costs about four credit frames per window.
//
// The window tunes itself. A producer marks the frame that uses up its window. If the application then runs
// out of data while the producer is waiting for a grant, each side is waiting on the other: the window,
// not the network or the producer, is limiting throughput (typically a link with a long round trip, or
// socket buffers larger than the window). The window then doubles, up to the maximum. A slow reader never
// runs out of data, so it never grows its window or buffers more.
sealed class SocketPullStream : Stream
{
  readonly Channel<SocketStreamChunk> Chunks;
  readonly Action<long> SendCredit;
  readonly Action OnDispose;
  readonly Action<long> ReleaseMemory;
  readonly long MaxWindow;
  SocketStreamChunk Current;
  int Offset;
  int Completed;
  int Disposed;

  // Producer-visible accounting. Granted is written by the consumer and read by the read loop; Received
  // is written by the read loop and read by the consumer.
  long Granted;
  long Received;
  // 1 when the latest frame from the producer used up its window, so it is stalled until a grant reaches it.
  int ProducerWaiting;

  // Consumer-only state.
  long Window;
  long Consumed;
  long ConsumedAtLastGrant;

  public SocketPullStream(
      long initialWindow,
      long maxWindow,
      Action<long> sendCredit,
      Action onDispose,
      Action<long> releaseMemory = null)
  {
    Chunks = Channel.CreateUnbounded<SocketStreamChunk>(new UnboundedChannelOptions
    {
      SingleReader = true,
      SingleWriter = false
    });
    Window = initialWindow;
    MaxWindow = Math.Max(initialWindow, maxWindow);
    Granted = initialWindow;
    SendCredit = sendCredit;
    OnDispose = onDispose;
    ReleaseMemory = releaseMemory;
  }

  // The current window. Read it only from the consumer's thread, or after reading has finished.
  internal long WindowBytes => Window;

  // Bytes the producer may still send. Read loop only.
  internal long RemainingWindow => Interlocked.Read(ref Granted) - Volatile.Read(ref Received);

  public override bool CanRead => true;
  public override bool CanSeek => false;
  public override bool CanWrite => false;
  public override long Length => throw new NotSupportedException();

  public override long Position
  {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }

  // Threading contract: the producer side (TryWriteChunk, Complete) runs on the connection's read loop or
  // close path; the consumer side (ReadAsync, Dispose) runs on the caller's thread. Only the consumer ever
  // touches Current or reads from the channel. The producer side only writes to and completes the
  // channel, so it can never release a buffer the consumer is still copying from.

  // Read loop. Returns false when the chunk exceeds the granted window (a protocol violation). reserved is
  // what the chunk holds against the connection's memory budget; it is given back once the chunk has been
  // read or dropped.
  public bool TryWriteChunk(SocketPayload payload, long reserved, bool producerWaiting)
  {
    var chunk = new SocketStreamChunk(payload, reserved, ReleaseMemory);
    var received = Volatile.Read(ref Received) + payload.Length;
    Volatile.Write(ref Received, received);
    if (received > Interlocked.Read(ref Granted))
    {
      chunk.Dispose();
      return false;
    }

    // Tracks the latest frame: data that keeps arriving unflagged means the producer has window to spare.
    Volatile.Write(ref ProducerWaiting, producerWaiting ? 1 : 0);

    if (!Chunks.Writer.TryWrite(chunk))
    {
      // The consumer disposed the stream; chunks still in flight before the producer sees the cancel
      // are dropped.
      chunk.Dispose();
    }

    return true;
  }

  public void Complete()
  {
    Volatile.Write(ref Completed, 1);
    Chunks.Writer.TryComplete();
  }

  // Data already buffered is still delivered; the consumer sees the exception after the last chunk.
  public void Complete(Exception exception)
  {
    Volatile.Write(ref Completed, 1);
    Chunks.Writer.TryComplete(exception);
  }

  public override int Read(byte[] buffer, int offset, int count)
  {
    return ReadAsync(buffer.AsMemory(offset, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();
  }

  public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
  {
    if (buffer.Length == 0)
      return 0;

    while (Offset >= Current.Length)
    {
      Current.Dispose();
      Current = default;
      if (Chunks.Reader.TryRead(out var next))
      {
        Current = next;
        Offset = 0;
        continue;
      }

      OnBufferEmpty();
      if (!await Chunks.Reader.WaitToReadAsync(cancellationToken))
        return 0;
    }

    var count = Math.Min(buffer.Length, Current.Length - Offset);
    Current.Memory.Slice(Offset, count).CopyTo(buffer);
    Offset += count;
    OnConsumed(count);
    return count;
  }

  void OnConsumed(int count)
  {
    Consumed += count;
    if (Consumed - ConsumedAtLastGrant >= Window / 4)
      Grant(extra: 0);
  }

  // The application is about to wait for data. If the producer's latest frame used up its window, the
  // producer is stalled until a grant reaches it while this side waits for its data: the window is the
  // bottleneck. Double it and grant the increase, plus anything read since the last grant, at once. The
  // flag is cleared so one stall grows the window once; the next exhausted frame re-arms it.
  void OnBufferEmpty()
  {
    if (Volatile.Read(ref Completed) != 0 || Interlocked.Exchange(ref ProducerWaiting, 0) == 0)
      return;

    var extra = Math.Min(Window, MaxWindow - Window);
    Window += extra;
    Grant(extra);
  }

  void Grant(long extra)
  {
    if (Volatile.Read(ref Completed) != 0)
      return;

    var amount = Consumed - ConsumedAtLastGrant + extra;
    if (amount <= 0)
      return;

    ConsumedAtLastGrant = Consumed;
    Interlocked.Add(ref Granted, amount);
    SendCredit(amount);
  }

  protected override void Dispose(bool disposing)
  {
    if (disposing && Interlocked.Exchange(ref Disposed, 1) == 0)
    {
      // Only tell the producer to stop if it has not already finished.
      var notifyProducer = Interlocked.Exchange(ref Completed, 1) == 0;
      Chunks.Writer.TryComplete();
      Current.Dispose();
      Current = default;
      DrainChunks();
      if (notifyProducer)
        OnDispose?.Invoke();
    }

    base.Dispose(disposing);
  }

  public override ValueTask DisposeAsync()
  {
    Dispose(true);
    GC.SuppressFinalize(this);
    return ValueTask.CompletedTask;
  }

  public override void Flush()
  {
  }

  public override Task FlushAsync(CancellationToken cancellationToken)
  {
    return Task.CompletedTask;
  }

  public override long Seek(long offset, SeekOrigin origin)
  {
    throw new NotSupportedException();
  }

  public override void SetLength(long value)
  {
    throw new NotSupportedException();
  }

  public override void Write(byte[] buffer, int offset, int count)
  {
    throw new NotSupportedException();
  }

  void DrainChunks()
  {
    while (Chunks.Reader.TryRead(out var chunk))
      chunk.Dispose();
  }

  readonly struct SocketStreamChunk : IDisposable
  {
    readonly SocketPayload Payload;
    readonly long Reserved;
    readonly Action<long> ReleaseMemory;

    public SocketStreamChunk(SocketPayload payload, long reserved, Action<long> releaseMemory)
    {
      Payload = payload;
      Reserved = reserved;
      ReleaseMemory = releaseMemory;
    }

    public ReadOnlyMemory<byte> Memory => Payload.Memory;
    public int Length => Memory.Length;

    public void Dispose()
    {
      Payload.Dispose();
      if (Reserved > 0)
        ReleaseMemory?.Invoke(Reserved);
    }
  }
}
