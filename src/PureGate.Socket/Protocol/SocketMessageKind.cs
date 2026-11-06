namespace PureGate.Socket.Protocol;

public enum SocketMessageKind
{
  Request = 1,
  Response = 2,
  Event = 3,
  Ping = 4,
  Pong = 5,
  Close = 6,
  Cancel = 7,
  Chunk = 8,
  StreamRequest = 9,
  StreamStart = 10,
  StreamItem = 11,
  StreamEnd = 12,

  // Flow control. MessageId 0 grants message credits for the connection; any other MessageId grants
  // bytes to that stream. The frame's value carries the amount.
  Credit = 15
}
