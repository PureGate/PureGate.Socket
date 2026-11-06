using System.Buffers.Binary;
using System.Text;

namespace PureGate.Socket.Protocol;

public sealed record SocketBatchItem(
    int Id,
    string Service,
    string Operation,
    byte[] Payload);

public sealed record SocketBatchResult(
    int Id,
    bool Succeeded,
    byte[] Payload,
    string ErrorCode,
    string ErrorMessage);

static class SocketBatchCodec
{
  // Smallest encoding of one entry: every length-prefixed field present but empty. A declared count larger
  // than the remaining bytes allow is rejected before anything is allocated for it.
  const int MinItemBytes = 4 + 4 + 4 + 4;
  const int MinResultBytes = 4 + 1 + 4 + 4 + 4;

  // Encoded once into a pooled buffer of the exact size; the caller disposes it after sending.
  public static SocketPayload SerializeItems(IReadOnlyList<SocketBatchItem> items)
  {
    long length = 4;
    foreach (var item in items ?? [])
      length += 4 + StringLength(item.Service) + StringLength(item.Operation) + 4 + (item.Payload?.Length ?? 0);

    var payload = SocketPayload.Rent(CheckedLength(length));
    var span = System.Runtime.InteropServices.MemoryMarshal.AsMemory(payload.Memory).Span;
    var offset = 0;
    WriteInt32(span, ref offset, items?.Count ?? 0);
    foreach (var item in items ?? [])
    {
      WriteInt32(span, ref offset, item.Id);
      WriteString(span, ref offset, item.Service);
      WriteString(span, ref offset, item.Operation);
      WriteBytes(span, ref offset, item.Payload);
    }

    return payload;
  }

  public static IReadOnlyList<SocketBatchItem> DeserializeItems(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var count = ReadInt32(payload, ref offset);
    if (count < 0 || count > (payload.Length - offset) / MinItemBytes)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid batch item count.");

    var items = new SocketBatchItem[count];
    for (var i = 0; i < count; i++)
    {
      items[i] = new SocketBatchItem(
          ReadInt32(payload, ref offset),
          ReadString(payload, ref offset),
          ReadString(payload, ref offset),
          ReadBytes(payload, ref offset));
    }

    EnsureConsumed(payload, offset);
    return items;
  }

  // Encoded once into an array of the exact size (it becomes a response payload, which may be cached).
  public static byte[] SerializeResults(IReadOnlyList<SocketBatchResult> results)
  {
    long length = 4;
    foreach (var result in results ?? [])
      length += 4 + 1 + 4 + (result.Payload?.Length ?? 0) + StringLength(result.ErrorCode) + StringLength(result.ErrorMessage);

    var output = GC.AllocateUninitializedArray<byte>(CheckedLength(length));
    var span = output.AsSpan();
    var offset = 0;
    WriteInt32(span, ref offset, results?.Count ?? 0);
    foreach (var result in results ?? [])
    {
      WriteInt32(span, ref offset, result.Id);
      span[offset++] = result.Succeeded ? (byte)1 : (byte)0;
      WriteBytes(span, ref offset, result.Payload);
      WriteString(span, ref offset, result.ErrorCode);
      WriteString(span, ref offset, result.ErrorMessage);
    }

    return output;
  }

  public static IReadOnlyList<SocketBatchResult> DeserializeResults(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var count = ReadInt32(payload, ref offset);
    if (count < 0 || count > (payload.Length - offset) / MinResultBytes)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid batch result count.");

    var results = new SocketBatchResult[count];
    for (var i = 0; i < count; i++)
    {
      var id = ReadInt32(payload, ref offset);
      var succeeded = ReadByte(payload, ref offset) != 0;
      results[i] = new SocketBatchResult(
          id,
          succeeded,
          ReadBytes(payload, ref offset),
          ReadString(payload, ref offset),
          ReadString(payload, ref offset));
    }

    EnsureConsumed(payload, offset);
    return results;
  }

  static int StringLength(string value)
  {
    return 4 + (string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetByteCount(value));
  }

  static int CheckedLength(long length)
  {
    if (length > Array.MaxLength)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Batch is too large to encode.");
    return (int)length;
  }

  static void WriteInt32(Span<byte> output, ref int offset, int value)
  {
    BinaryPrimitives.WriteInt32BigEndian(output.Slice(offset, 4), value);
    offset += 4;
  }

  static void WriteString(Span<byte> output, ref int offset, string value)
  {
    if (string.IsNullOrEmpty(value))
    {
      WriteInt32(output, ref offset, 0);
      return;
    }

    var length = Encoding.UTF8.GetBytes(value, output.Slice(offset + 4));
    WriteInt32(output, ref offset, length);
    offset += length;
  }

  static void WriteBytes(Span<byte> output, ref int offset, byte[] value)
  {
    value ??= [];
    WriteInt32(output, ref offset, value.Length);
    value.CopyTo(output.Slice(offset));
    offset += value.Length;
  }

  static byte ReadByte(ReadOnlySpan<byte> payload, ref int offset)
  {
    EnsureRemaining(payload, offset, 1);
    return payload[offset++];
  }

  static int ReadInt32(ReadOnlySpan<byte> payload, ref int offset)
  {
    EnsureRemaining(payload, offset, 4);
    var value = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(offset, 4));
    offset += 4;
    return value;
  }

  static string ReadString(ReadOnlySpan<byte> payload, ref int offset)
  {
    var length = ReadInt32(payload, ref offset);
    if (length == 0)
      return string.Empty;
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid batch string length.");

    EnsureRemaining(payload, offset, length);
    var value = Encoding.UTF8.GetString(payload.Slice(offset, length));
    offset += length;
    return value;
  }

  static byte[] ReadBytes(ReadOnlySpan<byte> payload, ref int offset)
  {
    var length = ReadInt32(payload, ref offset);
    if (length == 0)
      return [];
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid batch payload length.");

    EnsureRemaining(payload, offset, length);
    var value = payload.Slice(offset, length).ToArray();
    offset += length;
    return value;
  }

  static void EnsureConsumed(ReadOnlySpan<byte> payload, int offset)
  {
    if (offset != payload.Length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Batch payload contains trailing bytes.");
  }

  static void EnsureRemaining(ReadOnlySpan<byte> payload, int offset, int length)
  {
    if (length < 0 || offset < 0 || offset > payload.Length || payload.Length - offset < length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Batch payload is truncated.");
  }
}
