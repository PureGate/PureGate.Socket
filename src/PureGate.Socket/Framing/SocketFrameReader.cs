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

  public async ValueTask<SocketFrame> ReadAsync(CancellationToken cancellationToken)
  {
    while (true)
    {
      var result = await Reader.ReadAsync(cancellationToken);
      var buffer = result.Buffer;
      if (TryReadFrame(buffer, out var frame, out var consumed))
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
    }
  }

  bool TryReadFrame(
      ReadOnlySequence<byte> buffer,
      out SocketFrame frame,
      out SequencePosition consumed)
  {
    frame = null;
    consumed = buffer.Start;
    var reader = new SequenceReader<byte>(buffer);
    if (!reader.TryReadBigEndian(out int length))
      return false;

    if (length < 1 || length > maxFrameSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame size is invalid.");

    if (reader.Remaining < length)
      return false;

    var payload = buffer.Slice(reader.Position, length);
    consumed = buffer.GetPosition(length, reader.Position);
    frame = Decode(payload);
    return true;
  }

  SocketFrame Decode(ReadOnlySequence<byte> payload)
  {
    if (payload.IsSingleSegment)
      return Decode(payload.FirstSpan);

    var length = checked((int)payload.Length);
    var buffer = ArrayPool<byte>.Shared.Rent(length);
    try
    {
      payload.CopyTo(buffer);
      return Decode(buffer.AsSpan(0, length));
    }
    finally
    {
      ArrayPool<byte>.Shared.Return(buffer);
    }
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

    var value = buffer.Slice(offset, length).ToArray();
    offset += length;
    return SocketPayload.Wrap(value);
  }

  static void EnsureRemaining(ReadOnlySpan<byte> buffer, int offset, int length)
  {
    if (length < 0 || offset < 0 || offset > buffer.Length || buffer.Length - offset < length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame is truncated.");
  }
}
