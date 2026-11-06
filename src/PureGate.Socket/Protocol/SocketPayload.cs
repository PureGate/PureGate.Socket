using System.Buffers;

namespace PureGate.Socket.Protocol;

internal readonly struct SocketPayload : IDisposable
{
  readonly byte[] Buffer;
  readonly int Offset;
  readonly SocketPayloadOwner Owner;

  SocketPayload(byte[] buffer, int offset, int length, SocketPayloadOwner owner)
  {
    if (length < 0 || offset < 0 || offset > (buffer?.Length ?? 0) || (buffer?.Length ?? 0) - offset < length)
      throw new ArgumentOutOfRangeException(nameof(length));

    Buffer = buffer ?? [];
    Offset = offset;
    Length = length;
    Owner = owner;
  }

  public static SocketPayload Empty { get; } = new([], 0, 0, null);

  public int Length { get; }
  public bool IsEmpty => Length == 0;

  public ReadOnlyMemory<byte> Memory => Length == 0
      ? ReadOnlyMemory<byte>.Empty
      : Buffer.AsMemory(Offset, Length);

  public ReadOnlySpan<byte> Span => Length == 0
      ? ReadOnlySpan<byte>.Empty
      : Buffer.AsSpan(Offset, Length);

  public static SocketPayload Wrap(byte[] value)
  {
    return Wrap(value, 0, value?.Length ?? 0);
  }

  public static SocketPayload Wrap(byte[] value, int length)
  {
    return Wrap(value, 0, length);
  }

  public static SocketPayload Wrap(byte[] value, int offset, int length)
  {
    return length == 0
        ? Empty
        : new SocketPayload(value ?? [], offset, length, null);
  }

  public static SocketPayload Copy(ReadOnlySpan<byte> value)
  {
    if (value.Length == 0)
      return Empty;

    var payload = Rent(value.Length);
    value.CopyTo(payload.Owner.Buffer.AsSpan(0, value.Length));
    return payload;
  }

  public static SocketPayload Rent(int length)
  {
    if (length < 0)
      throw new ArgumentOutOfRangeException(nameof(length));
    if (length == 0)
      return Empty;

    var owner = SocketPayloadOwner.Rent(length);
    return new SocketPayload(owner.Buffer, 0, length, owner);
  }

  public SocketPayload Retain()
  {
    Owner?.Retain();
    return this;
  }

  public byte[] ToArray()
  {
    if (Length == 0)
      return [];

    var value = new byte[Length];
    Span.CopyTo(value);
    return value;
  }

  public void Dispose()
  {
    Owner?.Release();
  }

  sealed class SocketPayloadOwner
  {
    int ReferenceCount = 1;

    SocketPayloadOwner(byte[] buffer)
    {
      Buffer = buffer;
    }

    public byte[] Buffer { get; }

    public static SocketPayloadOwner Rent(int length)
    {
      return new SocketPayloadOwner(ArrayPool<byte>.Shared.Rent(length));
    }

    public void Retain()
    {
      while (true)
      {
        var current = Volatile.Read(ref ReferenceCount);
        if (current <= 0)
          throw new ObjectDisposedException(nameof(SocketPayload));

        if (Interlocked.CompareExchange(ref ReferenceCount, current + 1, current) == current)
          return;
      }
    }

    public void Release()
    {
      if (Interlocked.Decrement(ref ReferenceCount) == 0)
        ArrayPool<byte>.Shared.Return(Buffer);
    }
  }
}
