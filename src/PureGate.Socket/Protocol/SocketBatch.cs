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
  public static byte[] SerializeItems(IReadOnlyList<SocketBatchItem> items)
  {
    using var output = new MemoryStream();
    WriteInt32(output, items?.Count ?? 0);
    if (items is null)
      return output.ToArray();

    foreach (var item in items)
    {
      WriteInt32(output, item.Id);
      WriteString(output, item.Service);
      WriteString(output, item.Operation);
      WriteBytes(output, item.Payload);
    }

    return output.ToArray();
  }

  public static IReadOnlyList<SocketBatchItem> DeserializeItems(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var count = ReadInt32(payload, ref offset);
    if (count < 0)
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

  public static byte[] SerializeResults(IReadOnlyList<SocketBatchResult> results)
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

  public static IReadOnlyList<SocketBatchResult> DeserializeResults(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var count = ReadInt32(payload, ref offset);
    if (count < 0)
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

  static void WriteInt32(Stream stream, int value)
  {
    Span<byte> buffer = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(buffer, value);
    stream.Write(buffer);
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
