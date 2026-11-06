using System.Runtime.CompilerServices;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

sealed class SocketFrameReader(
    Stream stream,
    int maxFrameSizeBytes,
    SocketPayloadCompression compression = SocketPayloadCompression.None,
    int maxDecompressedPayloadSizeBytes = 128 * 1024 * 1024)
{
  readonly PipeReader Reader = PipeReader.Create(stream);

  const int MinimumReadHint = 4096;

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
            : throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame is truncated.");
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

    if (length < 1 || length > maxFrameSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame size is invalid.");

    if (reader.Remaining < length)
    {
      required = 4L + length;
      return false;
    }

    var payload = buffer.Slice(reader.Position, length);
    consumed = buffer.GetPosition(length, reader.Position);
    frame = Decode(payload);
    return true;
  }

  SocketFrame Decode(ReadOnlySequence<byte> payload)
  {
    return payload.IsSingleSegment
        ? Decode(payload.FirstSpan)
        : DecodeSegmented(payload);
  }

  // A frame split across pipe segments (common for large frames). The header is parsed in place and the
  // payload is copied straight into its final buffer, so the payload is copied once, not twice.
  SocketFrame DecodeSegmented(ReadOnlySequence<byte> payload)
  {
    var reader = new SequenceReader<byte>(payload);
    var version = ReadInt32(ref reader);
    var kind = (SocketMessageKind)ReadByte(ref reader);
    var fast = kind is SocketMessageKind.FastRequest or SocketMessageKind.FastResponse;
    var messageId = ReadInt64(ref reader);
    var operationId = ReadInt32(ref reader);
    var flags = ReadInt32(ref reader);
    var deadlineMs = ReadInt32(ref reader);
    var frame = new SocketFrame
    {
      Version = version,
      Kind = kind,
      MessageId = messageId,
      OperationId = operationId,
      Flags = flags,
      DeadlineMs = deadlineMs
    };

    if (fast)
    {
      frame.Ok = ReadByte(ref reader) != 0;
      frame.ErrorCode = kind == SocketMessageKind.FastResponse ? ReadString(ref reader) : string.Empty;
      frame.ErrorMessage = kind == SocketMessageKind.FastResponse ? ReadString(ref reader) : string.Empty;
    }
    else
    {
      frame.OriginalKind = (SocketMessageKind)ReadByte(ref reader);
      frame.ChunkIndex = ReadInt32(ref reader);
      frame.ChunkCount = ReadInt32(ref reader);
      frame.TotalPayloadLength = ReadInt32(ref reader);
      frame.Ok = ReadByte(ref reader) != 0;
      frame.Service = ReadString(ref reader);
      frame.Operation = ReadString(ref reader);
      frame.IdempotencyKey = ReadString(ref reader);
      frame.ErrorCode = ReadString(ref reader);
      frame.ErrorMessage = ReadString(ref reader);
    }

    var framePayload = ReadBytes(ref reader, pooled: kind == SocketMessageKind.StreamItem);
    if (!reader.End)
    {
      framePayload.Dispose();
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame contains trailing bytes.");
    }

    frame.SetPayload(framePayload);
    return FinishFrame(frame);
  }

  static byte ReadByte(ref SequenceReader<byte> reader)
  {
    if (!reader.TryRead(out var value))
      throw Truncated();
    return value;
  }

  static int ReadInt32(ref SequenceReader<byte> reader)
  {
    if (!reader.TryReadBigEndian(out int value))
      throw Truncated();
    return value;
  }

  static long ReadInt64(ref SequenceReader<byte> reader)
  {
    if (!reader.TryReadBigEndian(out long value))
      throw Truncated();
    return value;
  }

  static string ReadString(ref SequenceReader<byte> reader)
  {
    var length = ReadInt32(ref reader);
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame string length is invalid.");
    if (reader.Remaining < length)
      throw Truncated();
    if (length == 0)
      return string.Empty;

    var value = Encoding.UTF8.GetString(reader.UnreadSequence.Slice(0, length));
    reader.Advance(length);
    return value;
  }

  static SocketPayload ReadBytes(ref SequenceReader<byte> reader, bool pooled)
  {
    var length = ReadInt32(ref reader);
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame payload length is invalid.");
    if (reader.Remaining < length)
      throw Truncated();
    if (length == 0)
      return SocketPayload.Empty;

    var source = reader.UnreadSequence.Slice(0, length);
    SocketPayload payload;
    if (pooled)
    {
      payload = SocketPayload.Rent(length);
      source.CopyTo(MemoryMarshalWritable(payload, length));
    }
    else
    {
      var value = GC.AllocateUninitializedArray<byte>(length);
      source.CopyTo(value);
      payload = SocketPayload.Wrap(value);
    }

    reader.Advance(length);
    return payload;
  }

  static Span<byte> MemoryMarshalWritable(SocketPayload payload, int length)
  {
    return System.Runtime.InteropServices.MemoryMarshal.AsMemory(payload.Memory).Span.Slice(0, length);
  }

  static SocketProtocolException Truncated()
  {
    return new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame is truncated.");
  }

  SocketFrame Decode(ReadOnlySpan<byte> payload)
  {
    var offset = 0;
    var version = ReadInt32(payload, ref offset);
    var kind = (SocketMessageKind)ReadByte(payload, ref offset);
    if (kind is SocketMessageKind.FastRequest or SocketMessageKind.FastResponse)
      return DecodeFast(payload, ref offset, version, kind);

    var messageId = ReadInt64(payload, ref offset);
    var operationId = ReadInt32(payload, ref offset);
    var flags = ReadInt32(payload, ref offset);
    var deadlineMs = ReadInt32(payload, ref offset);
    var originalKind = (SocketMessageKind)ReadByte(payload, ref offset);
    var chunkIndex = ReadInt32(payload, ref offset);
    var chunkCount = ReadInt32(payload, ref offset);
    var totalPayloadLength = ReadInt32(payload, ref offset);
    var ok = ReadByte(payload, ref offset) != 0;
    var service = ReadString(payload, ref offset);
    var operation = ReadString(payload, ref offset);
    var idempotencyKey = ReadString(payload, ref offset);
    var errorCode = ReadString(payload, ref offset);
    var errorMessage = ReadString(payload, ref offset);
    var framePayload = ReadBytes(payload, ref offset, kind == SocketMessageKind.StreamItem);
    if (offset != payload.Length)
    {
      framePayload.Dispose();
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame contains trailing bytes.");
    }

    var frame = new SocketFrame
    {
      Version = version,
      Kind = kind,
      OriginalKind = originalKind,
      MessageId = messageId,
      OperationId = operationId,
      Flags = flags,
      DeadlineMs = deadlineMs,
      ChunkIndex = chunkIndex,
      ChunkCount = chunkCount,
      TotalPayloadLength = totalPayloadLength,
      Ok = ok,
      Service = service,
      Operation = operation,
      IdempotencyKey = idempotencyKey,
      ErrorCode = errorCode,
      ErrorMessage = errorMessage
    };
    frame.SetPayload(framePayload);
    return FinishFrame(frame);
  }

  SocketFrame DecodeFast(
      ReadOnlySpan<byte> payload,
      ref int offset,
      int version,
      SocketMessageKind kind)
  {
    var messageId = ReadInt64(payload, ref offset);
    var operationId = ReadInt32(payload, ref offset);
    var flags = ReadInt32(payload, ref offset);
    var deadlineMs = ReadInt32(payload, ref offset);
    var ok = ReadByte(payload, ref offset) != 0;
    var errorCode = kind == SocketMessageKind.FastResponse ? ReadString(payload, ref offset) : string.Empty;
    var errorMessage = kind == SocketMessageKind.FastResponse ? ReadString(payload, ref offset) : string.Empty;
    var framePayload = ReadBytes(payload, ref offset, pooled: false);
    if (offset != payload.Length)
    {
      framePayload.Dispose();
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame contains trailing bytes.");
    }

    var frame = new SocketFrame
    {
      Version = version,
      Kind = kind,
      MessageId = messageId,
      OperationId = operationId,
      Flags = flags,
      DeadlineMs = deadlineMs,
      Ok = ok,
      ErrorCode = errorCode,
      ErrorMessage = errorMessage
    };
    frame.SetPayload(framePayload);
    return FinishFrame(frame);
  }

  // Shared final step of every decoder: rejects unknown flags and decompresses a compressed payload.
  SocketFrame FinishFrame(SocketFrame frame)
  {
    try
    {
      if ((frame.Flags & ~SocketFrameFlags.Known) != 0)
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame contains unsupported flags.");

      if ((frame.Flags & SocketFrameFlags.PayloadCompressed) != 0)
      {
        if (compression != SocketPayloadCompression.Brotli)
          throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Compressed payload is not supported by this peer.");

        frame.Payload = DecompressBrotli(frame.PayloadMemory.ToArray());
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

  byte[] DecompressBrotli(byte[] payload)
  {
    using var input = new MemoryStream(payload);
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

  static byte ReadByte(ReadOnlySpan<byte> buffer, ref int offset)
  {
    EnsureRemaining(buffer, offset, 1);
    return buffer[offset++];
  }

  static int ReadInt32(ReadOnlySpan<byte> buffer, ref int offset)
  {
    EnsureRemaining(buffer, offset, 4);
    var value = BinaryPrimitives.ReadInt32BigEndian(buffer.Slice(offset, 4));
    offset += 4;
    return value;
  }

  static long ReadInt64(ReadOnlySpan<byte> buffer, ref int offset)
  {
    EnsureRemaining(buffer, offset, 8);
    var value = BinaryPrimitives.ReadInt64BigEndian(buffer.Slice(offset, 8));
    offset += 8;
    return value;
  }

  static string ReadString(ReadOnlySpan<byte> buffer, ref int offset)
  {
    var length = ReadInt32(buffer, ref offset);
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame string length is invalid.");
    EnsureRemaining(buffer, offset, length);
    var value = length == 0
        ? string.Empty
        : Encoding.UTF8.GetString(buffer.Slice(offset, length));
    offset += length;
    return value;
  }

  static SocketPayload ReadBytes(ReadOnlySpan<byte> buffer, ref int offset, bool pooled)
  {
    var length = ReadInt32(buffer, ref offset);
    if (length < 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame payload length is invalid.");
    EnsureRemaining(buffer, offset, length);
    if (length == 0)
      return SocketPayload.Empty;

    if (pooled)
    {
      var payload = SocketPayload.Copy(buffer.Slice(offset, length));
      offset += length;
      return payload;
    }

    // Uninitialized: every byte is overwritten by the copy below before anything can read it.
    var value = GC.AllocateUninitializedArray<byte>(length);
    buffer.Slice(offset, length).CopyTo(value);
    offset += length;
    return SocketPayload.Wrap(value);
  }

  static void EnsureRemaining(ReadOnlySpan<byte> buffer, int offset, int length)
  {
    if (length < 0 || offset < 0 || offset > buffer.Length || buffer.Length - offset < length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame is truncated.");
  }
}
