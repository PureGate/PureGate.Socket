using System.Collections.Concurrent;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// Flow control. Two kinds of credit, both granted by the receiver with Credit frames:
//
// Message credits (per connection). The handshake tells each side how many handler-bound messages
// (requests, fast requests, events, stream requests) the other can queue: its MaxQueuedHandlers. A sender
// spends one credit per message and waits when it has none; the receiver returns credits as its handlers
// pick messages up. The receiver's queue therefore never overflows, so a busy receiver slows its senders
// down instead of rejecting requests or closing the connection on an event burst.
//
// Stream credits (per stream, in bytes). See SocketPullStream (consumer) and StreamSendWindow (producer).
//
// Together they let the read loop accept everything that arrives without ever waiting: what a peer may
// send is bounded by what it was granted, and a peer that sends more is breaking the protocol.
public sealed partial class SocketPeer
{
  readonly MessageCredits MessageCredits;
  readonly ConcurrentDictionary<long, StreamSendWindow> OutgoingStreams = new();
  int UnreturnedMessageCredits;

  // Returned credits are batched: one Credit frame per quarter of the window.
  int MessageCreditBatch => Math.Max(1, Options.MaxQueuedHandlers / 4);

  // Gives a credit back when the frame it was taken for never reached the write lock (the caller gave up
  // first, or the connection closed), so a cancelled send does not leak window.
  void RefundUnsentMessage(SocketFrame frame)
  {
    if (!frame.WriteStarted)
      MessageCredits.Release(1);
  }

  // A handler picked a message up, freeing its queue slot.
  void ReturnMessageCredit()
  {
    if (Interlocked.Increment(ref UnreturnedMessageCredits) < MessageCreditBatch)
      return;

    var amount = Interlocked.Exchange(ref UnreturnedMessageCredits, 0);
    if (amount > 0)
      SendCreditInBackground(0, amount);
  }

  // Read loop: a Credit frame from the remote.
  void DispatchCredit(SocketFrame frame)
  {
    if (frame.Value <= 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Credit frame carries no credit.");

    if (frame.MessageId == 0)
    {
      MessageCredits.Release(frame.Value);
      return;
    }

    // A grant for a stream that already ended is harmless and ignored.
    if (OutgoingStreams.TryGetValue(frame.MessageId, out var window))
      window.Grant(frame.Value);
  }

  internal void SendCreditInBackground(long messageId, long amount)
  {
    // Grants are capped well below int.MaxValue by the window limits, so this never truncates.
    SendControlInBackground(new SocketFrame
    {
      MessageId = messageId,
      Kind = SocketMessageKind.Credit,
      Value = (int)amount
    });
  }

  // Control frames (credits, pongs, rejections) are written in the background, so the thread that
  // triggers them (often the read loop) never waits on the socket.
  void SendControlInBackground(SocketFrame frame)
  {
    if (Lifetime.IsCancellationRequested)
      return;

    _ = SendControlAsync(frame);
  }

  async Task SendControlAsync(SocketFrame frame)
  {
    try
    {
      await WriteFrameAsync(frame, Lifetime.Token);
    }
    catch
    {
      // The connection is closing or broken; the read loop reports that.
    }
  }

  // The handshake's proof (client to server) and ready (server to client) frames carry the sender's
  // message window in their value.
  internal static int ReadHandshakeWindow(SocketFrame frame)
  {
    if (frame.Value <= 0)
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Handshake carries no flow-control window.");
    return frame.Value;
  }
}
