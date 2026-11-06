using System.Threading.Channels;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

sealed class SocketPullStream : Stream
{
  readonly Channel<SocketStreamChunk> Chunks;
  readonly Action OnDispose;
  SocketStreamChunk Current;
  int Offset;
  int Completed;
  int Disposed;

  public SocketPullStream(int maxBufferedChunks, Action onDispose)
  {
    Chunks = Channel.CreateBounded<SocketStreamChunk>(new BoundedChannelOptions(maxBufferedChunks)
    {
      FullMode = BoundedChannelFullMode.Wait,
      SingleReader = true,
      SingleWriter = false
    });
    OnDispose = onDispose;
  }

  public override bool CanRead => true;
  public override bool CanSeek => false;
  public override bool CanWrite => false;
  public override long Length => throw new NotSupportedException();

  public override long Position
  {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }

  // Threading contract: the producer side (WriteChunkAsync, Complete) runs on the connection's read loop
  // or close path; the consumer side (ReadAsync, Dispose) runs on the caller's thread. Only the consumer
  // ever touches Current or reads from the channel. The producer side only writes to and completes the
  // channel, so it can never release a buffer the consumer is still copying from.

  public async ValueTask WriteChunkAsync(
      SocketPayload payload,
      CancellationToken cancellationToken)
  {
    var chunk = new SocketStreamChunk(payload);
    try
    {
      await Chunks.Writer.WriteAsync(chunk, cancellationToken);
    }
    catch (ChannelClosedException)
    {
      // The consumer disposed the stream; chunks still in flight before the producer sees the cancel
      // are dropped. This must not fault the read loop, which would close the whole connection.
      chunk.Dispose();
    }
    catch
    {
      chunk.Dispose();
      throw;
    }
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
      if (!await Chunks.Reader.WaitToReadAsync(cancellationToken))
        return 0;

      if (!Chunks.Reader.TryRead(out Current))
        continue;

      Offset = 0;
    }

    var count = Math.Min(buffer.Length, Current.Length - Offset);
    Current.Memory.Slice(Offset, count).CopyTo(buffer);
    Offset += count;
    return count;
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

    public SocketStreamChunk(SocketPayload payload)
    {
      Payload = payload;
    }

    public ReadOnlyMemory<byte> Memory => Payload.Memory;
    public int Length => Memory.Length;

    public void Dispose()
    {
      Payload.Dispose();
    }
  }
}
