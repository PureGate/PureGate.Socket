using System.Text;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

// The wire format. See docs/PROTOCOL.md for the full description.
//
//   frame   = length:uint32-be  body
//   body    = kind:u8  flags:u8  id:zigzag-varint
//             [method]  [deadline:varint]  [idempotency-key:str]  [value:zigzag-varint]  [error]
//             [chunk-fields]  payload:rest-of-body
//   method  = code:varint, then service:str operation:str when code is 0 (literal) or odd (definition)
//   error   = code-index:varint (0 = custom, then code:str)  message:str
//   str     = byte-length:varint  utf8-bytes
//
// A bracketed field is present only when its flag is set. Chunk fields (original-kind:u8 index:varint
// count:varint total:varint) are present only on Chunk frames.
static class SocketWire
{
  public const int Compressed = 1 << 0;
  public const int WindowExhausted = 1 << 1;
  public const int Ok = 1 << 2;
  public const int HasMethod = 1 << 3;
  public const int HasDeadline = 1 << 4;
  public const int HasIdempotencyKey = 1 << 5;
  public const int HasValue = 1 << 6;
  public const int HasError = 1 << 7;

  // Method table limits, per connection and direction. A sender that has defined MaxMethods methods
  // sends further names literally; a receiver rejects definitions past the limit.
  public const int MaxMethods = 4096;
  public const int MaxMethodNameBytes = 1024;

  static readonly string[] KnownErrorCodes =
  [
    SocketProtocolError.Timeout,
    SocketProtocolError.Cancelled,
    SocketProtocolError.Unauthorized,
    SocketProtocolError.UnsupportedVersion,
    SocketProtocolError.UnsupportedOperation,
    SocketProtocolError.PayloadTooLarge,
    SocketProtocolError.ProtocolError,
    SocketProtocolError.HandlerFailed,
    SocketProtocolError.ConnectionClosed,
    SocketProtocolError.Overloaded
  ];

  // 1-based index of a library error code, or 0 for a custom code (sent as text).
  public static int ErrorCodeIndex(string code)
  {
    for (var i = 0; i < KnownErrorCodes.Length; i++)
    {
      if (string.Equals(KnownErrorCodes[i], code, StringComparison.Ordinal))
        return i + 1;
    }

    return 0;
  }

  public static string ErrorCodeAt(ulong index)
  {
    if (index == 0 || index > (ulong)KnownErrorCodes.Length)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Frame carries an unknown error code.");
    return KnownErrorCodes[index - 1];
  }

  public static ulong ZigZag(long value)
  {
    return (ulong)((value << 1) ^ (value >> 63));
  }

  public static long UnZigZag(ulong value)
  {
    return (long)(value >> 1) ^ -(long)(value & 1);
  }

  public static int VarintLength(ulong value)
  {
    var length = 1;
    while (value >= 0x80)
    {
      value >>= 7;
      length++;
    }

    return length;
  }

  public static void WriteVarint(Span<byte> buffer, ref int offset, ulong value)
  {
    while (value >= 0x80)
    {
      buffer[offset++] = (byte)(value | 0x80);
      value >>= 7;
    }

    buffer[offset++] = (byte)value;
  }

  public static int Utf8Length(string value)
  {
    return string.IsNullOrEmpty(value) ? 0 : Encoding.UTF8.GetByteCount(value);
  }

  public static int StringLength(int utf8Length)
  {
    return VarintLength((ulong)utf8Length) + utf8Length;
  }

  public static void WriteString(Span<byte> buffer, ref int offset, string value, int utf8Length)
  {
    WriteVarint(buffer, ref offset, (ulong)utf8Length);
    if (utf8Length > 0)
      offset += Encoding.UTF8.GetBytes(value, buffer.Slice(offset, utf8Length));
  }
}
