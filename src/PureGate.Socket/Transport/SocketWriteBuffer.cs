using System.Buffers;

namespace PureGate.Socket.Transport;

// The frames a connection has encoded and not yet written to the socket: one contiguous array rented
// from the shared pool, so a whole batch goes out in one socket write. Not thread-safe; SocketPeer uses it
// under its write lock.
sealed class SocketWriteBuffer : IBufferWriter<byte>
{
  const int InitialBytes = 16 * 1024;

  byte[] Buffer = [];
  int Written;

  public int Length => Written;

  public void Advance(int count)
  {
    if (count < 0 || Written + count > Buffer.Length)
      throw new ArgumentOutOfRangeException(nameof(count));
    Written += count;
  }

  public Memory<byte> GetMemory(int sizeHint = 0)
  {
    Ensure(sizeHint);
    return Buffer.AsMemory(Written);
  }

  public Span<byte> GetSpan(int sizeHint = 0)
  {
    Ensure(sizeHint);
    return Buffer.AsSpan(Written);
  }

  // Hands the written bytes over (the caller returns the array to the pool, or gives it back as spare
  // for a later Take) and starts again on spare, or on a fresh array when spare is null.
  public ArraySegment<byte> Take(byte[] spare)
  {
    var batch = new ArraySegment<byte>(Buffer, 0, Written);
    Buffer = spare ?? [];
    Written = 0;
    return batch;
  }

  void Ensure(int sizeHint)
  {
    var needed = Math.Max(1, sizeHint);
    if (Buffer.Length - Written >= needed)
      return;

    var size = (int)Math.Min(Array.MaxLength, Math.Max((long)Written + needed, Math.Max(InitialBytes, Buffer.Length * 2L)));
    var grown = ArrayPool<byte>.Shared.Rent(size);
    Buffer.AsSpan(0, Written).CopyTo(grown);
    if (Buffer.Length > 0)
      ArrayPool<byte>.Shared.Return(Buffer);
    Buffer = grown;
  }
}
