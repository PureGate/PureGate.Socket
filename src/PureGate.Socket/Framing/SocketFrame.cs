using PureGate.Socket.Protocol;

namespace PureGate.Socket.Framing;

sealed class SocketFrame
{
  SocketPayload PayloadValue;

  public int Version { get; set; }
  public long MessageId { get; set; }
  public SocketMessageKind Kind { get; set; }
  public SocketMessageKind OriginalKind { get; set; }
  public int OperationId { get; set; }
  public string Service { get; set; }
  public string Operation { get; set; }
  public string IdempotencyKey { get; set; }
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
