using System.Buffers;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Serialization;

// An IBufferWriter<byte> over buffers rented from ArrayPool<byte>.Shared. Codecs serialize into it; the
// result is either handed over as a pooled payload (returned to the pool when the frame has been sent)
// or copied once into an exact-length array.
//
// One instance per thread is reused. Serialization is synchronous, so an instance is only in use between
// the start and end of one Serialize call; a nested call on the same thread gets a fresh instance.
sealed class SocketPayloadWriter : IBufferWriter<byte>
{
  const int InitialBytes = 512;

  [ThreadStatic]
  static SocketPayloadWriter Cached;

  byte[] Buffer = [];
  int Written;

  public static SocketPayload Serialize<T>(ISocketMessageCodec codec, T value)
  {
    var writer = Take();
    try
    {
      codec.Serialize(value, writer);
      return writer.Detach();
    }
    finally
    {
      writer.Return();
    }
  }

  public static byte[] SerializeToArray<T>(ISocketMessageCodec codec, T value)
  {
    var writer = Take();
    try
    {
      codec.Serialize(value, writer);
      return writer.Written == 0 ? [] : writer.Buffer.AsSpan(0, writer.Written).ToArray();
    }
    finally
    {
      writer.Return();
    }
  }

  // Encodes with a caller-supplied writer function into a pooled payload.
  public static SocketPayload Write<TState>(TState state, Action<TState, IBufferWriter<byte>> write)
  {
    var writer = Take();
    try
    {
      write(state, writer);
      return writer.Detach();
    }
    finally
    {
      writer.Return();
    }
  }

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

  // Hands the written bytes over as a pooled payload; the writer starts empty again.
  SocketPayload Detach()
  {
    if (Written == 0)
      return SocketPayload.Empty;

    var payload = SocketPayload.Adopt(Buffer, Written);
    Buffer = [];
    Written = 0;
    return payload;
  }

  static SocketPayloadWriter Take()
  {
    var writer = Cached ?? new SocketPayloadWriter();
    Cached = null;
    return writer;
  }

  // Keeps a small buffer for the next call on this thread; a large one goes back to the pool.
  void Return()
  {
    Written = 0;
    if (Buffer.Length > 16 * 1024)
    {
      ArrayPool<byte>.Shared.Return(Buffer);
      Buffer = [];
    }

    Cached = this;
  }
}
