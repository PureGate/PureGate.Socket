using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace PureGate.Socket.Protocol;

public sealed record SocketFastBatchItem(
    int Id,
    int OperationId,
    byte[] Payload);

public sealed record SocketFastBatchResult(
    int Id,
    bool Succeeded,
    byte[] Payload,
    string ErrorCode,
    string ErrorMessage);

readonly record struct SocketFastBatchItemView(
    int Id,
    int OperationId,
    ReadOnlyMemory<byte> Payload);

static class SocketFastBatchCodec
{
  // Smallest encoding of one entry; see SocketBatchCodec.
  const int MinItemBytes = 4 + 4 + 4;
  const int MinResultBytes = 4 + 1 + 4 + 4 + 4;

  public static byte[] SerializeItems(IReadOnlyList<SocketFastBatchItem> items)
  {
    using var output = new MemoryStream();
    WriteInt32(output, items?.Count ?? 0);
    if (items is null)
      return output.ToArray();

    foreach (var item in items)
    {
      WriteInt32(output, item.Id);
      WriteInt32(output, item.OperationId);
      WriteBytes(output, item.Payload);
    }

    return output.ToArray();
  }

  public static IReadOnlyList<SocketFastBatchItem> DeserializeItems(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var count = ReadInt32(payload, ref offset);
    if (count < 0 || count > (payload.Length - offset) / MinItemBytes)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid fast batch item count.");

    var items = new SocketFastBatchItem[count];
    for (var i = 0; i < count; i++)
    {
      items[i] = new SocketFastBatchItem(
          ReadInt32(payload, ref offset),
          ReadInt32(payload, ref offset),
          ReadBytes(payload, ref offset));
    }

    EnsureConsumed(payload, offset);
    return items;
  }

  public static int ReadItemCount(ReadOnlyMemory<byte> payload, out int offset)
  {
    offset = 0;
    var count = ReadInt32(payload.Span, ref offset);
    if (count < 0 || count > (payload.Length - offset) / MinItemBytes)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid fast batch item count.");

    return count;
  }

  public static SocketFastBatchItemView ReadItem(ReadOnlyMemory<byte> payload, ref int offset)
  {
    var span = payload.Span;
    var id = ReadInt32(span, ref offset);
    var operationId = ReadInt32(span, ref offset);
    var itemPayload = ReadMemory(payload, ref offset);
    return new SocketFastBatchItemView(id, operationId, itemPayload);
  }

  public static void EnsureConsumed(ReadOnlyMemory<byte> payload, int offset)
  {
    EnsureConsumed(payload.Span, offset);
  }

  public static byte[] SerializeResults(IReadOnlyList<SocketFastBatchResult> results)
  {
    using var output = new MemoryStream();
    WriteInt32(output, results?.Count ?? 0);
    if (results is null)
      return output.ToArray();

    foreach (var result in results)
    {
      WriteInt32(output, result.Id);
      output.WriteByte(result.Succeeded ? (byte)1 : (byte)0);
      WriteBytes(output, result.Payload);
      WriteString(output, result.ErrorCode);
      WriteString(output, result.ErrorMessage);
    }

    return output.ToArray();
  }

  public static IReadOnlyList<SocketFastBatchResult> DeserializeResults(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var count = ReadInt32(payload, ref offset);
    if (count < 0 || count > (payload.Length - offset) / MinResultBytes)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid fast batch result count.");

    var results = new SocketFastBatchResult[count];
    for (var i = 0; i < count; i++)
    {
      var id = ReadInt32(payload, ref offset);
      var succeeded = ReadByte(payload, ref offset) != 0;
      results[i] = new SocketFastBatchResult(
          id,
          succeeded,
          ReadBytes(payload, ref offset),
          ReadString(payload, ref offset),
          ReadString(payload, ref offset));
    }

    EnsureConsumed(payload, offset);
    return results;
  }

  static void WriteInt32(Stream stream, int value)
  {
    Span<byte> buffer = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(buffer, value);
    stream.Write(buffer);
  }

  public sealed class ResultWriter
  {
    readonly ArrayBufferWriter<byte> Writer = new();

    public ResultWriter(int count)
    {
      WriteInt32(count);
    }

    public byte[] ToArray()
    {
      return Writer.WrittenSpan.ToArray();
    }

    public void WriteResult(
        int id,
        bool succeeded,
        ReadOnlyMemory<byte> payload,
        string errorCode,
        string errorMessage)
    {
      WriteInt32(id);
      WriteByte(succeeded ? (byte)1 : (byte)0);
      WriteBytes(payload.Span);
      WriteString(errorCode);
      WriteString(errorMessage);
    }

    void WriteByte(byte value)
    {
      var span = Writer.GetSpan(1);
      span[0] = value;
      Writer.Advance(1);
    }

    void WriteInt32(int value)
    {
      var span = Writer.GetSpan(4);
      BinaryPrimitives.WriteInt32BigEndian(span.Slice(0, 4), value);
      Writer.Advance(4);
    }

    void WriteString(string value)
    {
      if (string.IsNullOrEmpty(value))
      {
        WriteInt32(0);
        return;
      }

      var length = Encoding.UTF8.GetByteCount(value);
      WriteInt32(length);
      var span = Writer.GetSpan(length);
      Writer.Advance(Encoding.UTF8.GetBytes(value, span.Slice(0, length)));
    }

    void WriteBytes(ReadOnlySpan<byte> value)
    {
      WriteInt32(value.Length);
      if (value.Length == 0)
        return;

      var span = Writer.GetSpan(value.Length);
      value.CopyTo(span.Slice(0, value.Length));
      Writer.Advance(value.Length);
    }
  }

  static void WriteString(Stream stream, string value)
  {
    if (string.IsNullOrEmpty(value))
    {
      WriteInt32(stream, 0);
      return;
    }

    var bytes = Encoding.UTF8.GetBytes(value);
    WriteInt32(stream, bytes.Length);
    stream.Write(bytes);
  }

  static void WriteBytes(Stream stream, byte[] value)
  {
    value ??= [];
    WriteInt32(stream, value.Length);
    stream.Write(value);
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
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid fast batch string length.");

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
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid fast batch payload length.");

    EnsureRemaining(payload, offset, length);
    var value = payload.Slice(offset, length).ToArray();
    offset += length;
    return value;
  }

  static ReadOnlyMemory<byte> ReadMemory(ReadOnlyMemory<byte> payload, ref int offset)
  {
    var span = payload.Span;
    var length = ReadInt32(span, ref offset);
    if (length == 0)
      return ReadOnlyMemory<byte>.Empty;
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Invalid fast batch payload length.");

    EnsureRemaining(span, offset, length);
    var value = payload.Slice(offset, length);
    offset += length;
    return value;
  }

  static void EnsureConsumed(ReadOnlySpan<byte> payload, int offset)
  {
    if (offset != payload.Length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Fast batch payload contains trailing bytes.");
  }

  static void EnsureRemaining(ReadOnlySpan<byte> payload, int offset, int length)
  {
    if (length < 0 || offset < 0 || offset > payload.Length || payload.Length - offset < length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Fast batch payload is truncated.");
  }
}
