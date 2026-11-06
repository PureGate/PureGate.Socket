using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

// Decodes frames in the wire format (see SocketWire). Used by one reader at a time: the connection's read
// loop (or the handshake before it). It owns the receive side of the method table.
sealed class SocketFrameReader(
    Stream stream,
    int maxFrameSizeBytes,
    SocketPayloadCompression compression = SocketPayloadCompression.None,
    int maxDecompressedPayloadSizeBytes = 128 * 1024 * 1024)
{
  const int MinimumReadHint = 4096;

  readonly PipeReader Reader = PipeReader.Create(stream);
  readonly List<(string Service, string Operation)> Methods = [];

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  public async ValueTask<SocketFrame> ReadAsync(CancellationToken cancellationToken)
  {
    var readHint = 0;
    while (true)
    {
      // Once a frame's length is known, ask the pipe for more of it per read, so a large frame arrives in a
      // few big reads instead of many small ones. The hint grows with what has actually arrived (at most
      // twice the buffered bytes plus 4 KB), so a peer that only announces a large frame cannot make this
      // side reserve memory for bytes it never sends.
      var result = readHint > 0
          ? await Reader.ReadAtLeastAsync(readHint, cancellationToken)
          : await Reader.ReadAsync(cancellationToken);
      var buffer = result.Buffer;
      if (TryReadFrame(buffer, out var frame, out var consumed, out var required))
      {
        Reader.AdvanceTo(consumed);
        return frame;
      }

      if (result.IsCompleted)
      {
        Reader.AdvanceTo(buffer.End);
        return buffer.Length == 0
            ? null
            : throw Truncated();
      }

      Reader.AdvanceTo(buffer.Start, buffer.End);
      readHint = required <= 0
          ? 0
          : (int)Math.Min(required, buffer.Length * 2L + MinimumReadHint);
    }
  }

  // required is the total number of bytes the frame in progress needs (length prefix included), or 0 when
  // even the length prefix has not arrived yet.
  bool TryReadFrame(
      ReadOnlySequence<byte> buffer,
      out SocketFrame frame,
      out SequencePosition consumed,
      out long required)
  {
    frame = null;
    consumed = buffer.Start;
    required = 0;
    var reader = new SequenceReader<byte>(buffer);
    if (!reader.TryReadBigEndian(out int length))
      return false;

    if (length < 2 || length > maxFrameSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame size is invalid.");

    if (reader.Remaining < length)
    {
      required = 4L + length;
      return false;
    }

    var body = buffer.Slice(reader.Position, length);
    consumed = buffer.GetPosition(length, reader.Position);
    frame = DecodeBody(body, alias: null);
    return true;
  }

  // Decodes one frame body. When alias is the array the body lives in (a reassembled chunked frame, which
  // nobody else uses), the payload is exposed in place instead of being copied.
  internal SocketFrame DecodeBody(ReadOnlySequence<byte> body, byte[] alias)
  {
    var reader = new SequenceReader<byte>(body);
    var kind = (SocketMessageKind)ReadByte(ref reader);
    if (!Enum.IsDefined(kind))
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Unknown frame kind.");

    int flags = ReadByte(ref reader);
    var frame = new SocketFrame
    {
      Kind = kind,
      MessageId = SocketWire.UnZigZag(ReadVarint(ref reader)),
      Ok = (flags & SocketWire.Ok) != 0,
      Flags = flags & SocketFrameFlags.Known,
      Service = string.Empty,
      Operation = string.Empty,
      IdempotencyKey = string.Empty
    };

    if ((flags & SocketWire.HasMethod) != 0)
      ReadMethod(ref reader, frame);
    if ((flags & SocketWire.HasDeadline) != 0)
      frame.DeadlineMs = ReadInt32(ref reader);
    if ((flags & SocketWire.HasIdempotencyKey) != 0)
      frame.IdempotencyKey = ReadString(ref reader);
    if ((flags & SocketWire.HasValue) != 0)
      frame.Value = checked((int)SocketWire.UnZigZag(ReadVarint(ref reader)));
    if ((flags & SocketWire.HasError) != 0)
    {
      var index = ReadVarint(ref reader);
      frame.ErrorCode = index == 0 ? ReadString(ref reader) : SocketWire.ErrorCodeAt(index);
      frame.ErrorMessage = ReadString(ref reader);
    }

    if (kind == SocketMessageKind.Chunk)
    {
      frame.OriginalKind = (SocketMessageKind)ReadByte(ref reader);
      frame.ChunkIndex = ReadInt32(ref reader);
      frame.ChunkCount = ReadInt32(ref reader);
      frame.TotalPayloadLength = ReadInt32(ref reader);
    }

    frame.SetPayload(ReadPayload(ref reader, kind, alias));
    return FinishFrame(frame);
  }

  void ReadMethod(ref SequenceReader<byte> reader, SocketFrame frame)
  {
    var code = ReadVarint(ref reader);
    if (code == 0)
    {
      frame.Service = ReadMethodName(ref reader);
      frame.Operation = ReadMethodName(ref reader);
      return;
    }

    var id = code >> 1;
    if ((code & 1) != 0)
    {
      // Definitions arrive in order, numbered from 1, so the table only ever grows by one.
      if (id != (ulong)Methods.Count + 1 || id > SocketWire.MaxMethods)
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame defines a method out of order or past the limit.");

      frame.Service = ReadMethodName(ref reader);
      frame.Operation = ReadMethodName(ref reader);
      Methods.Add((frame.Service, frame.Operation));
      return;
    }

    if (id == 0 || id > (ulong)Methods.Count)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame references an undefined method.");

    (frame.Service, frame.Operation) = Methods[(int)id - 1];
  }

  static string ReadMethodName(ref SequenceReader<byte> reader)
  {
    var length = ReadVarint(ref reader);
    if (length > SocketWire.MaxMethodNameBytes)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Method name exceeds the limit.");
    return ReadStringBytes(ref reader, length);
  }

  static SocketPayload ReadPayload(ref SequenceReader<byte> reader, SocketMessageKind kind, byte[] alias)
  {
    var length = checked((int)reader.Remaining);
    if (length == 0)
      return SocketPayload.Empty;

    if (alias is not null && kind != SocketMessageKind.StreamItem)
    {
      var offset = checked((int)reader.Consumed);
      reader.Advance(length);
      return SocketPayload.Wrap(alias, offset, length);
    }

    var source = reader.UnreadSequence;
    SocketPayload payload;
    if (kind == SocketMessageKind.StreamItem)
    {
      // Stream data is pooled; the pull stream returns the buffer once the application has read it.
      payload = SocketPayload.Rent(length);
      source.CopyTo(System.Runtime.InteropServices.MemoryMarshal.AsMemory(payload.Memory).Span);
    }
    else
    {
      // Uninitialized: every byte is overwritten by the copy before anything can read it.
      var value = GC.AllocateUninitializedArray<byte>(length);
      source.CopyTo(value);
      payload = SocketPayload.Wrap(value);
    }

    reader.Advance(length);
    return payload;
  }

  // Final step of every decode: decompresses a compressed payload.
  SocketFrame FinishFrame(SocketFrame frame)
  {
    try
    {
      if ((frame.Flags & SocketFrameFlags.PayloadCompressed) != 0)
      {
        if (compression != SocketPayloadCompression.Brotli)
          throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Compressed payload is not supported by this peer.");

        frame.Payload = DecompressBrotli(frame.PayloadMemory.Span);
        frame.Flags &= ~SocketFrameFlags.PayloadCompressed;
      }
    }
    catch
    {
      frame.ReleasePayload();
      throw;
    }

    return frame;
  }

  byte[] DecompressBrotli(ReadOnlySpan<byte> payload)
  {
    using var input = new MemoryStream(payload.ToArray());
    using var brotli = new BrotliStream(input, CompressionMode.Decompress);
    using var output = new MemoryStream();
    var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
    try
    {
      while (true)
      {
        var read = brotli.Read(buffer, 0, buffer.Length);
        if (read == 0)
          break;

        if (output.Length + read > maxDecompressedPayloadSizeBytes)
          throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Decompressed payload exceeds the configured maximum size.");

        output.Write(buffer, 0, read);
      }
    }
    finally
    {
      ArrayPool<byte>.Shared.Return(buffer);
    }

    return output.ToArray();
  }

  static byte ReadByte(ref SequenceReader<byte> reader)
  {
    if (!reader.TryRead(out var value))
      throw Truncated();
    return value;
  }

  static ulong ReadVarint(ref SequenceReader<byte> reader)
  {
    ulong value = 0;
    for (var shift = 0; shift < 64; shift += 7)
    {
      var b = ReadByte(ref reader);
      value |= (ulong)(b & 0x7F) << shift;
      if ((b & 0x80) == 0)
        return value;
    }

    throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame contains an invalid varint.");
  }

  static int ReadInt32(ref SequenceReader<byte> reader)
  {
    var value = ReadVarint(ref reader);
    if (value > int.MaxValue)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame field is out of range.");
    return (int)value;
  }

  static string ReadString(ref SequenceReader<byte> reader)
  {
    return ReadStringBytes(ref reader, ReadVarint(ref reader));
  }

  static string ReadStringBytes(ref SequenceReader<byte> reader, ulong length)
  {
    if (length > (ulong)reader.Remaining)
      throw Truncated();
    if (length == 0)
      return string.Empty;

    var value = Encoding.UTF8.GetString(reader.UnreadSequence.Slice(0, (long)length));
    reader.Advance((long)length);
    return value;
  }

  static SocketProtocolException Truncated()
  {
    return new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame is truncated.");
  }
}
