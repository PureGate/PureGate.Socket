using System.Buffers;

namespace PureGate.Socket.Transport;

// The frames a connection has encoded and not yet written to the socket, in 64 KB segments rented from the
// shared pool. Each segment goes to the socket in one write. Two reasons for the size:
// - A write larger than the socket's send buffer stays pending until the remote has read nearly all of it,
//   and the remote then receives it in small pieces, one completion each. Measured: 64 KB payloads from 64
//   callers ran at a third of the throughput with whole-batch writes, and worse with 256 KB writes.
// - A 64 KB array stays below the large object heap threshold, so even a pool miss is a cheap
//   allocation, and a large message is spread over several segments instead of needing one array of its
//   size (which, under load, churned the large object heap and forced full collections).
// Not thread-safe; SocketPeer uses it under its write lock.
sealed class SocketWriteBuffer : IBufferWriter<byte>
{
  public const int SegmentBytes = 64 * 1024;

  readonly List<ArraySegment<byte>> Filled = [];
  byte[] Current;
  int CurrentWritten;

  public long Length { get; private set; }

  public void Advance(int count)
  {
    if (count < 0 || Current is null || CurrentWritten + count > Current.Length)
      throw new ArgumentOutOfRangeException(nameof(count));
    CurrentWritten += count;
    Length += count;
  }

  public Memory<byte> GetMemory(int sizeHint = 0)
  {
    Ensure(sizeHint);
    return Current.AsMemory(CurrentWritten);
  }

  public Span<byte> GetSpan(int sizeHint = 0)
  {
    Ensure(sizeHint);
    return Current.AsSpan(CurrentWritten);
  }

  // Moves everything written into batch, in order, and starts empty.
  public void MoveTo(SocketWriteBatch batch)
  {
    foreach (var segment in Filled)
      batch.Add(segment);
    Filled.Clear();
    if (CurrentWritten > 0)
    {
      batch.Add(new ArraySegment<byte>(Current, 0, CurrentWritten));
      Current = null;
      CurrentWritten = 0;
    }

    Length = 0;
  }

  // Returns every segment to the pool, written or not.
  public void Release()
  {
    foreach (var segment in Filled)
      ArrayPool<byte>.Shared.Return(segment.Array);
    Filled.Clear();
    if (Current is not null)
      ArrayPool<byte>.Shared.Return(Current);
    Current = null;
    CurrentWritten = 0;
    Length = 0;
  }

  void Ensure(int sizeHint)
  {
    var needed = Math.Max(1, sizeHint);
    if (Current is not null && Current.Length - CurrentWritten >= needed)
      return;

    // The current segment is full (or too small for this request): seal it and start another.
    if (Current is not null)
    {
      if (CurrentWritten > 0)
        Filled.Add(new ArraySegment<byte>(Current, 0, CurrentWritten));
      else
        ArrayPool<byte>.Shared.Return(Current);
    }

    Current = ArrayPool<byte>.Shared.Rent(Math.Max(SegmentBytes, needed));
    CurrentWritten = 0;
  }
}

// A batch being written to the socket: the segments taken from SocketWriteBuffer, in order. Owned by the
// connection's flusher, one batch at a time.
sealed class SocketWriteBatch
{
  readonly List<ArraySegment<byte>> Segments = [];

  public long Length { get; private set; }
  public bool IsEmpty => Segments.Count == 0;

  public void Add(ArraySegment<byte> segment)
  {
    Segments.Add(segment);
    Length += segment.Count;
  }

  // Writes the segments in order, each in one socket write.
  public ValueTask WriteToAsync(Stream stream, CancellationToken cancellationToken)
  {
    return Segments.Count == 1
        ? stream.WriteAsync(Segments[0].AsMemory(), cancellationToken)
        : WriteAllAsync(stream, cancellationToken);
  }

  async ValueTask WriteAllAsync(Stream stream, CancellationToken cancellationToken)
  {
    foreach (var segment in Segments)
      await stream.WriteAsync(segment.AsMemory(), cancellationToken);
  }

  // Returns the segments to the pool and empties the batch.
  public void Release()
  {
    foreach (var segment in Segments)
      ArrayPool<byte>.Shared.Return(segment.Array);
    Segments.Clear();
    Length = 0;
  }
}
