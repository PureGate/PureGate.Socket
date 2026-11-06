using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

sealed class SocketFrameWriter(
    Stream stream,
    int maxFrameSizeBytes,
    SocketPayloadCompression compression = SocketPayloadCompression.None,
    int compressionMinPayloadBytes = 16 * 1024)
{
  readonly PipeWriter Writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));

  public async ValueTask WriteAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteAsync(frame, frame?.PayloadMemory ?? ReadOnlyMemory<byte>.Empty, cancellationToken);
  }

  public async ValueTask WriteAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    var prepared = PrepareFrame(frame);
    var payload = payloadOverride;
    if (payloadOverride.IsEmpty && prepared.PayloadLength > 0)
      payload = prepared.PayloadMemory;
    if (ReferenceEquals(prepared, frame))
      prepared = PrepareFrame(prepared, payload);
    else
      payload = prepared.PayloadMemory;

    var length = GetEncodedLength(prepared, payload.Length);
    if (length > maxFrameSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame exceeds the configured maximum size.");

    var buffer = Writer.GetSpan(4 + length);
    BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(0, 4), length);
    Encode(prepared, payload.Span, buffer.Slice(4, length));
    Writer.Advance(4 + length);
    var flush = await Writer.FlushAsync(cancellationToken);
    if (flush.IsCanceled)
      throw new OperationCanceledException(cancellationToken);
  }

  SocketFrame PrepareFrame(SocketFrame frame)
  {
    return PrepareFrame(frame, frame?.PayloadMemory ?? ReadOnlyMemory<byte>.Empty);
  }

  SocketFrame PrepareFrame(SocketFrame frame, ReadOnlyMemory<byte> payloadMemory)
  {
    frame ??= new SocketFrame();
    if (compression != SocketPayloadCompression.Brotli || frame.Kind == SocketMessageKind.Chunk)
      return frame;

    var payload = payloadMemory;
    if (payload.Length < compressionMinPayloadBytes)
      return frame;

    var compressed = CompressBrotli(payload.Span);
    if (compressed.Length >= payload.Length)
      return frame;

    return new SocketFrame
    {
      Version = frame.Version,
      MessageId = frame.MessageId,
      Kind = frame.Kind,
      Service = frame.Service,
      Operation = frame.Operation,
      IdempotencyKey = frame.IdempotencyKey,
      Flags = frame.Flags | SocketFrameFlags.PayloadCompressed,
      DeadlineMs = frame.DeadlineMs,
      Ok = frame.Ok,
      ErrorCode = frame.ErrorCode,
      ErrorMessage = frame.ErrorMessage,
      Payload = compressed
    };
  }

  static byte[] CompressBrotli(ReadOnlySpan<byte> payload)
  {
    using var output = new MemoryStream();
    using (var brotli = new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true))
      brotli.Write(payload);
    return output.ToArray();
  }

  static int GetEncodedLength(SocketFrame frame, int payloadLength)
  {
    frame ??= new SocketFrame();
    if (frame.Kind is SocketMessageKind.FastRequest or SocketMessageKind.FastResponse)
      return GetFastEncodedLength(frame, payloadLength);

    var serviceLength = GetStringLength(frame.Service);
    var operationLength = GetStringLength(frame.Operation);
    var idempotencyKeyLength = GetStringLength(frame.IdempotencyKey);
    var errorCodeLength = GetStringLength(frame.ErrorCode);
    var errorMessageLength = GetStringLength(frame.ErrorMessage);
    var length =
        4 + // version
        1 + // kind
        8 + // message id
        4 + // operation id
        4 + // flags
        4 + // deadline
        1 + // original kind
        4 + // chunk index
        4 + // chunk count
        4 + // total payload length
        1 + // ok
        4 + serviceLength +
        4 + operationLength +
        4 + idempotencyKeyLength +
        4 + errorCodeLength +
        4 + errorMessageLength +
        4 + payloadLength;
    return length;
  }

  static int GetFastEncodedLength(SocketFrame frame, int payloadLength)
  {
    var errorCodeLength = frame.Kind == SocketMessageKind.FastResponse ? GetStringLength(frame.ErrorCode) : 0;
    var errorMessageLength = frame.Kind == SocketMessageKind.FastResponse ? GetStringLength(frame.ErrorMessage) : 0;
    return
        4 + // version
        1 + // kind
        8 + // message id
        4 + // operation id
        4 + // flags
        4 + // deadline
        1 + // ok
        (frame.Kind == SocketMessageKind.FastResponse ? 4 + errorCodeLength + 4 + errorMessageLength : 0) +
        4 + payloadLength;
  }

  static void Encode(SocketFrame frame, Span<byte> buffer)
  {
    Encode(frame, frame is null ? [] : frame.PayloadMemory.Span, buffer);
  }

  static void Encode(SocketFrame frame, ReadOnlySpan<byte> payload, Span<byte> buffer)
  {
    frame ??= new SocketFrame();
    if (frame.Kind is SocketMessageKind.FastRequest or SocketMessageKind.FastResponse)
    {
      EncodeFast(frame, payload, buffer);
      return;
    }

    var serviceLength = GetStringLength(frame.Service);
    var operationLength = GetStringLength(frame.Operation);
    var idempotencyKeyLength = GetStringLength(frame.IdempotencyKey);
    var errorCodeLength = GetStringLength(frame.ErrorCode);
    var errorMessageLength = GetStringLength(frame.ErrorMessage);
    var offset = 0;
    WriteInt32(buffer, ref offset, frame.Version);
    WriteByte(buffer, ref offset, (byte)frame.Kind);
    WriteInt64(buffer, ref offset, frame.MessageId);
    WriteInt32(buffer, ref offset, frame.OperationId);
    WriteInt32(buffer, ref offset, frame.Flags);
    WriteInt32(buffer, ref offset, frame.DeadlineMs);
    WriteByte(buffer, ref offset, (byte)frame.OriginalKind);
    WriteInt32(buffer, ref offset, frame.ChunkIndex);
    WriteInt32(buffer, ref offset, frame.ChunkCount);
    WriteInt32(buffer, ref offset, frame.TotalPayloadLength);
    WriteByte(buffer, ref offset, frame.Ok ? (byte)1 : (byte)0);
    WriteString(buffer, ref offset, frame.Service, serviceLength);
    WriteString(buffer, ref offset, frame.Operation, operationLength);
    WriteString(buffer, ref offset, frame.IdempotencyKey, idempotencyKeyLength);
    WriteString(buffer, ref offset, frame.ErrorCode, errorCodeLength);
    WriteString(buffer, ref offset, frame.ErrorMessage, errorMessageLength);
    WriteBytes(buffer, ref offset, payload);
  }

  static void EncodeFast(SocketFrame frame, ReadOnlySpan<byte> payload, Span<byte> buffer)
  {
    var offset = 0;
    WriteInt32(buffer, ref offset, frame.Version);
    WriteByte(buffer, ref offset, (byte)frame.Kind);
    WriteInt64(buffer, ref offset, frame.MessageId);
    WriteInt32(buffer, ref offset, frame.OperationId);
    WriteInt32(buffer, ref offset, frame.Flags);
    WriteInt32(buffer, ref offset, frame.DeadlineMs);
    WriteByte(buffer, ref offset, frame.Ok ? (byte)1 : (byte)0);
    if (frame.Kind == SocketMessageKind.FastResponse)
    {
      WriteString(buffer, ref offset, frame.ErrorCode, GetStringLength(frame.ErrorCode));
      WriteString(buffer, ref offset, frame.ErrorMessage, GetStringLength(frame.ErrorMessage));
    }

    WriteBytes(buffer, ref offset, payload);
  }

  static int GetStringLength(string value)
  {
    return string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetByteCount(value);
  }

  static void WriteByte(Span<byte> buffer, ref int offset, byte value)
  {
    buffer[offset++] = value;
  }

  static void WriteInt32(Span<byte> buffer, ref int offset, int value)
  {
    BinaryPrimitives.WriteInt32BigEndian(buffer.Slice(offset, 4), value);
    offset += 4;
  }

  static void WriteInt64(Span<byte> buffer, ref int offset, long value)
  {
    BinaryPrimitives.WriteInt64BigEndian(buffer.Slice(offset, 8), value);
    offset += 8;
  }

  static void WriteString(Span<byte> buffer, ref int offset, string value, int byteLength)
  {
    WriteInt32(buffer, ref offset, byteLength);
    if (byteLength == 0)
      return;

    offset += Encoding.UTF8.GetBytes(value, buffer.Slice(offset, byteLength));
  }

  static void WriteBytes(Span<byte> buffer, ref int offset, ReadOnlySpan<byte> value)
  {
    WriteInt32(buffer, ref offset, value.Length);
    if (value.Length == 0)
      return;

    value.CopyTo(buffer.Slice(offset, value.Length));
    offset += value.Length;
  }

}
