using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

sealed class SocketFrame
{
  SocketPayload PayloadValue;

  public long MessageId { get; set; }
  public SocketMessageKind Kind { get; set; }
  public SocketMessageKind OriginalKind { get; set; }
  // A small integer whose meaning depends on the kind: credit amount (Credit), initial stream window
  // (StreamRequest), message window (handshake proof and ready).
  public int Value { get; set; }
  public string Service { get; set; } = string.Empty;
  public string Operation { get; set; } = string.Empty;
  public string IdempotencyKey { get; set; } = string.Empty;
  public int Flags { get; set; }
  public int DeadlineMs { get; set; }
  public int ChunkIndex { get; set; }
  public int ChunkCount { get; set; }
  public int TotalPayloadLength { get; set; }
  public bool Ok { get; set; }
  public string ErrorCode { get; set; }
  public string ErrorMessage { get; set; }
  public byte[] Payload
  {
    get => PayloadValue.ToArray();
    set
    {
      PayloadValue.Dispose();
      PayloadValue = SocketPayload.Wrap(value ?? []);
    }
  }

  // Set once the frame holds the connection's write lock. From then on it reaches the wire (or the
  // connection closes), so a message credit spent on it is not given back.
  internal bool WriteStarted { get; set; }

  // Bytes this frame holds against its connection's memory budget. Whoever takes the payload over takes
  // this over too; SocketPeer.ReleaseFrame gives back what is left.
  internal long ReservedBytes { get; set; }

  internal bool PayloadIsPooled => PayloadValue.IsPooled;

  // Received payloads stay compressed until they are needed, so decompression happens off the read loop.
  internal bool IsCompressed => (Flags & SocketFrameFlags.PayloadCompressed) != 0;

  public int PayloadLength => PayloadValue.Length;
  public ReadOnlyMemory<byte> PayloadMemory => PayloadValue.Memory;

  internal void SetPayload(SocketPayload payload)
  {
    PayloadValue.Dispose();
    PayloadValue = payload;
  }

  internal SocketPayload TakePayload()
  {
    var payload = PayloadValue;
    PayloadValue = SocketPayload.Empty;
    return payload;
  }

  internal void ReleasePayload()
  {
    PayloadValue.Dispose();
    PayloadValue = SocketPayload.Empty;
  }
}
