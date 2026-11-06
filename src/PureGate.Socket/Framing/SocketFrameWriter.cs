using System.Buffers.Binary;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

// Encodes frames in the wire format (see SocketWire). Not thread-safe: SocketPeer calls it under its write
// lock, which is also what keeps method-table definitions in the same order as the frames on the wire.
//
// useMethodTable: when true, the first frame that uses a service/operation pair defines a number for it
// and later frames send only the number. Only the connection's own writer uses it; the handshake writer
// (and raw writers in tests) send names literally, so their frames never touch the connection's table.
sealed class SocketFrameWriter(
    Stream stream,
    int maxFrameSizeBytes,
    SocketPayloadCompression compression = SocketPayloadCompression.None,
    int compressionMinPayloadBytes = 16 * 1024,
    bool useMethodTable = false)
{
  readonly PipeWriter Writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
  readonly Dictionary<(string Service, string Operation), int> Methods = useMethodTable ? new() : null;

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  public async ValueTask WriteAsync(SocketFrame frame, CancellationToken cancellationToken)
  {
    await WriteAsync(frame, frame?.PayloadMemory ?? ReadOnlyMemory<byte>.Empty, cancellationToken);
  }

  // Encodes and flushes one frame that must fit in a single wire frame (handshake and test traffic).
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  public async ValueTask WriteAsync(
      SocketFrame frame,
      ReadOnlyMemory<byte> payloadOverride,
      CancellationToken cancellationToken)
  {
    if (Encode(frame, payloadOverride) is not null)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame exceeds the configured maximum size.");
    await FlushAsync(cancellationToken);
  }

  // Sends everything encoded so far.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  public async ValueTask FlushAsync(CancellationToken cancellationToken)
  {
    var flush = await Writer.FlushAsync(cancellationToken);
    if (flush.IsCanceled)
      throw new OperationCanceledException(cancellationToken);
  }

  // Encodes one frame into the pipe's buffer without sending it, and returns null. A frame too large for
  // one wire frame is not written: it is returned as an OversizedFrame for the caller to send in chunks.
  // Runs synchronously, so the payload is no longer read once it returns (except through the returned
  // OversizedFrame).
  //
  // The payload is payloadOverride when non-empty, otherwise the frame's own payload. Compression is
  // decided here and only changes the flags and bytes encoded; the frame itself is never modified.
  // Bytes encoded so far, length prefixes included. Written by the encoding thread (under the connection's
  // write lock), read by metrics.
  internal long BytesWritten;

  public OversizedFrame Encode(SocketFrame frame, ReadOnlyMemory<byte> payloadOverride)
  {
    frame ??= new SocketFrame();
    var payload = payloadOverride.IsEmpty ? frame.PayloadMemory : payloadOverride;
    var flags = frame.Flags & SocketFrameFlags.Known;
    if (ShouldCompress(frame.Kind, payload.Length))
    {
      var compressed = CompressBrotli(payload.Span);
      if (compressed.Length < payload.Length)
      {
        payload = compressed;
        flags |= SocketFrameFlags.PayloadCompressed;
      }
    }

    var method = PlanMethod(frame, allowDefinition: true);
    var layout = HeaderLayout.For(frame, flags, method);
    var bodyLength = (long)layout.Length + payload.Length;
    if (bodyLength <= maxFrameSizeBytes)
    {
      var span = Writer.GetSpan(4 + (int)bodyLength);
      BinaryPrimitives.WriteInt32BigEndian(span, (int)bodyLength);
      var offset = 4;
      layout.Write(span, ref offset, frame);
      payload.Span.CopyTo(span.Slice(offset));
      Writer.Advance(4 + (int)bodyLength);
      BytesWritten += 4 + bodyLength;
      if (method.Defines)
        Methods[(frame.Service, frame.Operation)] = method.Id;
      return null;
    }

    if (frame.Kind == SocketMessageKind.Chunk)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Frame exceeds the configured maximum size.");

    // Sent in chunks. Names are sent literally rather than defined: a definition must reach the receiver
    // before any frame that references it, and keeping chunked messages out of the table means that
    // stays true even if chunks of different messages are ever interleaved.
    var literal = HeaderLayout.For(frame, flags, PlanMethod(frame, allowDefinition: false));
    var header = new byte[literal.Length];
    var headerOffset = 0;
    literal.Write(header, ref headerOffset, frame);
    return new OversizedFrame(frame.Kind, frame.MessageId, header, payload);
  }

  // One chunk of an oversized frame. Its data is the slice [start, start + count) of the frame's encoded
  // body (header followed by payload), passed here as up to two spans.
  public void EncodeChunk(
      long messageId,
      SocketMessageKind originalKind,
      int index,
      int count,
      int total,
      ReadOnlySpan<byte> first,
      ReadOnlySpan<byte> second)
  {
    var id = SocketWire.ZigZag(messageId);
    var headerLength = 2
        + SocketWire.VarintLength(id)
        + 1
        + SocketWire.VarintLength((ulong)index)
        + SocketWire.VarintLength((ulong)count)
        + SocketWire.VarintLength((ulong)total);
    var bodyLength = headerLength + first.Length + second.Length;
    if (bodyLength > maxFrameSizeBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Chunk exceeds the configured maximum size.");

    var span = Writer.GetSpan(4 + bodyLength);
    BinaryPrimitives.WriteInt32BigEndian(span, bodyLength);
    var offset = 4;
    span[offset++] = (byte)SocketMessageKind.Chunk;
    span[offset++] = 0;
    SocketWire.WriteVarint(span, ref offset, id);
    span[offset++] = (byte)originalKind;
    SocketWire.WriteVarint(span, ref offset, (ulong)index);
    SocketWire.WriteVarint(span, ref offset, (ulong)count);
    SocketWire.WriteVarint(span, ref offset, (ulong)total);
    first.CopyTo(span.Slice(offset));
    second.CopyTo(span.Slice(offset + first.Length));
    Writer.Advance(4 + bodyLength);
    BytesWritten += 4 + bodyLength;
  }

  MethodPlan PlanMethod(SocketFrame frame, bool allowDefinition)
  {
    if (string.IsNullOrEmpty(frame.Service) && string.IsNullOrEmpty(frame.Operation))
      return default;

    var serviceBytes = SocketWire.Utf8Length(frame.Service);
    var operationBytes = SocketWire.Utf8Length(frame.Operation);
    if (serviceBytes > SocketWire.MaxMethodNameBytes || operationBytes > SocketWire.MaxMethodNameBytes)
      throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, $"Service and operation names are limited to {SocketWire.MaxMethodNameBytes} bytes.");

    if (Methods is not null && allowDefinition)
    {
      if (Methods.TryGetValue((frame.Service, frame.Operation), out var id))
        return new MethodPlan(true, (ulong)id << 1, false, 0, 0, id);
      if (Methods.Count < SocketWire.MaxMethods)
      {
        var next = Methods.Count + 1;
        return new MethodPlan(true, ((ulong)next << 1) | 1, true, serviceBytes, operationBytes, next);
      }
    }

    return new MethodPlan(true, 0, false, serviceBytes, operationBytes, 0);
  }

  bool ShouldCompress(SocketMessageKind kind, int payloadLength)
  {
    return compression == SocketPayloadCompression.Brotli
        && kind != SocketMessageKind.Chunk
        && payloadLength > 0
        && payloadLength >= compressionMinPayloadBytes;
  }

  static byte[] CompressBrotli(ReadOnlySpan<byte> payload)
  {
    using var output = new MemoryStream();
    using (var brotli = new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true))
      brotli.Write(payload);
    return output.ToArray();
  }

  // How a frame names its method. Code 0 sends the names literally; an odd code defines Id; an even code
  // references Id. Names are written for literals and definitions.
  readonly record struct MethodPlan(bool Present, ulong Code, bool Defines, int ServiceBytes, int OperationBytes, int Id)
  {
    public bool WritesNames => Present && (Code == 0 || Defines);
  }

  readonly struct HeaderLayout
  {
    readonly int Flags;
    readonly ulong Id;
    readonly MethodPlan Method;
    readonly int IdempotencyKeyBytes;
    readonly ulong Value;
    readonly int ErrorIndex;
    readonly int ErrorCodeBytes;
    readonly int ErrorMessageBytes;

    HeaderLayout(int flags, ulong id, MethodPlan method, int idempotencyKeyBytes, ulong value, int errorIndex, int errorCodeBytes, int errorMessageBytes, int length)
    {
      Flags = flags;
      Id = id;
      Method = method;
      IdempotencyKeyBytes = idempotencyKeyBytes;
      Value = value;
      ErrorIndex = errorIndex;
      ErrorCodeBytes = errorCodeBytes;
      ErrorMessageBytes = errorMessageBytes;
      Length = length;
    }

    public int Length { get; }

    public static HeaderLayout For(SocketFrame frame, int semanticFlags, MethodPlan method)
    {
      var flags = semanticFlags;
      var id = SocketWire.ZigZag(frame.MessageId);
      var length = 2 + SocketWire.VarintLength(id);
      if (frame.Ok)
        flags |= SocketWire.Ok;

      if (method.Present)
      {
        flags |= SocketWire.HasMethod;
        length += SocketWire.VarintLength(method.Code);
        if (method.WritesNames)
          length += SocketWire.StringLength(method.ServiceBytes) + SocketWire.StringLength(method.OperationBytes);
      }

      if (frame.DeadlineMs > 0)
      {
        flags |= SocketWire.HasDeadline;
        length += SocketWire.VarintLength((ulong)frame.DeadlineMs);
      }

      var idempotencyKeyBytes = 0;
      if (!string.IsNullOrEmpty(frame.IdempotencyKey))
      {
        flags |= SocketWire.HasIdempotencyKey;
        idempotencyKeyBytes = SocketWire.Utf8Length(frame.IdempotencyKey);
        length += SocketWire.StringLength(idempotencyKeyBytes);
      }

      var value = SocketWire.ZigZag(frame.Value);
      if (frame.Value != 0)
      {
        flags |= SocketWire.HasValue;
        length += SocketWire.VarintLength(value);
      }

      int errorIndex = 0, errorCodeBytes = 0, errorMessageBytes = 0;
      if (!string.IsNullOrEmpty(frame.ErrorCode) || !string.IsNullOrEmpty(frame.ErrorMessage))
      {
        flags |= SocketWire.HasError;
        errorIndex = SocketWire.ErrorCodeIndex(frame.ErrorCode);
        length += SocketWire.VarintLength((ulong)errorIndex);
        if (errorIndex == 0)
        {
          errorCodeBytes = SocketWire.Utf8Length(frame.ErrorCode);
          length += SocketWire.StringLength(errorCodeBytes);
        }

        errorMessageBytes = SocketWire.Utf8Length(frame.ErrorMessage);
        length += SocketWire.StringLength(errorMessageBytes);
      }

      if (frame.Kind == SocketMessageKind.Chunk)
      {
        length += 1
            + SocketWire.VarintLength((ulong)frame.ChunkIndex)
            + SocketWire.VarintLength((ulong)frame.ChunkCount)
            + SocketWire.VarintLength((ulong)frame.TotalPayloadLength);
      }

      return new HeaderLayout(flags, id, method, idempotencyKeyBytes, value, errorIndex, errorCodeBytes, errorMessageBytes, length);
    }

    public void Write(Span<byte> buffer, ref int offset, SocketFrame frame)
    {
      buffer[offset++] = (byte)frame.Kind;
      buffer[offset++] = (byte)Flags;
      SocketWire.WriteVarint(buffer, ref offset, Id);
      if (Method.Present)
      {
        SocketWire.WriteVarint(buffer, ref offset, Method.Code);
        if (Method.WritesNames)
        {
          SocketWire.WriteString(buffer, ref offset, frame.Service, Method.ServiceBytes);
          SocketWire.WriteString(buffer, ref offset, frame.Operation, Method.OperationBytes);
        }
      }

      if ((Flags & SocketWire.HasDeadline) != 0)
        SocketWire.WriteVarint(buffer, ref offset, (ulong)frame.DeadlineMs);
      if ((Flags & SocketWire.HasIdempotencyKey) != 0)
        SocketWire.WriteString(buffer, ref offset, frame.IdempotencyKey, IdempotencyKeyBytes);
      if ((Flags & SocketWire.HasValue) != 0)
        SocketWire.WriteVarint(buffer, ref offset, Value);
      if ((Flags & SocketWire.HasError) != 0)
      {
        SocketWire.WriteVarint(buffer, ref offset, (ulong)ErrorIndex);
        if (ErrorIndex == 0)
          SocketWire.WriteString(buffer, ref offset, frame.ErrorCode, ErrorCodeBytes);
        SocketWire.WriteString(buffer, ref offset, frame.ErrorMessage, ErrorMessageBytes);
      }

      if (frame.Kind == SocketMessageKind.Chunk)
      {
        buffer[offset++] = (byte)frame.OriginalKind;
        SocketWire.WriteVarint(buffer, ref offset, (ulong)frame.ChunkIndex);
        SocketWire.WriteVarint(buffer, ref offset, (ulong)frame.ChunkCount);
        SocketWire.WriteVarint(buffer, ref offset, (ulong)frame.TotalPayloadLength);
      }
    }
  }
}

// A frame whose encoded body (Header followed by Payload) does not fit in one wire frame.
sealed class OversizedFrame(SocketMessageKind kind, long messageId, byte[] header, ReadOnlyMemory<byte> payload)
{
  public SocketMessageKind Kind { get; } = kind;
  public long MessageId { get; } = messageId;
  public byte[] Header { get; } = header;
  public ReadOnlyMemory<byte> Payload { get; } = payload;
  public int TotalLength => Header.Length + Payload.Length;
}
