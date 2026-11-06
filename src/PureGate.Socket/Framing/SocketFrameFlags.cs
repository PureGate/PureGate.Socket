namespace PureGate.Socket.Framing;

static class SocketFrameFlags
{
  public const int PayloadCompressed = 1;

  // On a stream item: the producer has no window left after this frame and is waiting for a grant.
  public const int WindowExhausted = 2;

  public const int Known = PayloadCompressed | WindowExhausted;
}
