using System.Buffers;
using System.IO.Compression;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Framing;

// Payload decompression. Uses the span-based Brotli decoder over pooled buffers: no MemoryStream, no copy
// of the input, and output that is never allowed past the caller's limit, so a small "compression bomb"
// costs at most maxBytes of work and memory.
static class SocketCompression
{
  const int MinimumOutputBytes = 16 * 1024;

  // Decompresses source. Each output buffer is reserved from budget (when given) before it is allocated;
  // returns false when the budget refuses, having released everything it reserved. On success, reserved
  // is what the returned payload holds against the budget (its length, or its buffer when pooled).
  // Pooled output is a rented buffer the payload owns; otherwise it is an exact-length array.
  // Throws SocketProtocolException when the data is corrupt or would decompress past maxBytes.
  public static bool TryDecompress(
      ReadOnlySpan<byte> source,
      int maxBytes,
      SocketMemoryBudget budget,
      bool pooled,
      out SocketPayload payload,
      out long reserved)
  {
    payload = SocketPayload.Empty;
    reserved = 0;
    maxBytes = Math.Max(0, maxBytes);
    var size = (int)Math.Min(maxBytes, Math.Max(MinimumOutputBytes, source.Length * 4L));
    if (!Reserve(budget, size))
      return false;

    var held = (long)size;
    var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, size));
    var written = 0;
    var handedOff = false;
    try
    {
      using var decoder = new BrotliDecoder();
      Span<byte> probe = stackalloc byte[1];
      while (true)
      {
        var status = decoder.Decompress(source, buffer.AsSpan(written, size - written), out var consumed, out var produced);
        source = source[consumed..];
        written += produced;
        if (status == OperationStatus.Done)
        {
          if (!source.IsEmpty)
            throw Corrupt();
          break;
        }

        if (status != OperationStatus.DestinationTooSmall)
          throw Corrupt();

        if (size >= maxBytes)
        {
          // The output is full at the limit. The stream may still end right here; anything more is too much.
          status = decoder.Decompress(source, probe, out consumed, out produced);
          if (status == OperationStatus.Done && produced == 0 && consumed == source.Length)
            break;
          throw new SocketProtocolException(SocketProtocolError.PayloadTooLarge, "Decompressed payload exceeds the configured maximum size.");
        }

        var grown = (int)Math.Min(maxBytes, size * 2L);
        if (!Reserve(budget, grown - size))
          return false;
        held += grown - size;
        var next = ArrayPool<byte>.Shared.Rent(grown);
        buffer.AsSpan(0, written).CopyTo(next);
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = next;
        size = grown;
      }

      if (written == 0)
      {
        reserved = 0;
        return true;
      }

      if (pooled)
      {
        payload = SocketPayload.Adopt(buffer, written);
        handedOff = true;
        reserved = held;
        held = 0;
        return true;
      }

      // An exact-length array for payloads handed to application code, which may keep them.
      if (!Reserve(budget, written))
        return false;
      var value = GC.AllocateUninitializedArray<byte>(written);
      buffer.AsSpan(0, written).CopyTo(value);
      payload = SocketPayload.Wrap(value);
      reserved = written;
      return true;
    }
    catch (Exception ex) when (ex is InvalidOperationException or InvalidDataException or IOException)
    {
      throw Corrupt();
    }
    finally
    {
      budget?.Release(held);
      if (!handedOff)
        ArrayPool<byte>.Shared.Return(buffer);
    }
  }

  // Decompresses a frame's payload in place, with no budget. For callers outside a connection.
  public static void DecompressFrame(SocketFrame frame, int maxBytes)
  {
    if ((frame.Flags & SocketFrameFlags.PayloadCompressed) == 0)
      return;

    TryDecompress(frame.PayloadMemory.Span, maxBytes, budget: null, pooled: false, out var payload, out _);
    frame.SetPayload(payload);
    frame.Flags &= ~SocketFrameFlags.PayloadCompressed;
  }

  static bool Reserve(SocketMemoryBudget budget, long bytes)
  {
    return budget is null || budget.TryReserve(bytes);
  }

  static SocketProtocolException Corrupt()
  {
    // Corrupt compressed data is the peer's fault, so it is a protocol violation like any other.
    return new SocketProtocolException(SocketProtocolError.ProtocolError, "Compressed payload is corrupt.");
  }
}
