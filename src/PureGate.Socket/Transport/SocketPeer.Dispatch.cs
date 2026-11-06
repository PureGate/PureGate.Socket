using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  async Task ReadLoopAsync()
  {
    Exception closeException = null;
    try
    {
      while (!Lifetime.IsCancellationRequested)
      {
        var frame = await Reader.ReadAsync(Lifetime.Token);
        if (frame is null)
          break;

        Volatile.Write(ref LastReceivedTicks, Environment.TickCount64);
        ObserveFrameReceived(frame);
        PruneExpiredChunks();
        if (frame.Kind == SocketMessageKind.Chunk)
        {
          // A chunk's payload is a pooled buffer; reassembly keeps it, and any it did not keep goes back here.
          var chunk = frame;
          try
          {
            frame = ReassembleChunk(chunk);
          }
          finally
          {
            chunk.ReleasePayload();
          }

          if (frame is null)
            continue;
        }

        var releasePayload = true;
        try
        {
          releasePayload = Dispatch(frame);
        }
        finally
        {
          if (releasePayload)
            ReleaseFrame(frame);
        }
      }
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      closeException = ex;
    }
    finally
    {
      await CloseAsync(closeException);
    }
  }

  // Runs on the read loop and never waits: frames that need work go to handlers, frames that need a reply
  // are answered in the background, and everything else is applied in place. What a peer may send is
  // bounded by the credit it was granted, so the read loop can always keep draining the socket. That is
  // what keeps two peers that flood each other from deadlocking.
  bool Dispatch(SocketFrame frame)
  {
    switch (frame.Kind)
    {
      case SocketMessageKind.Request:
      case SocketMessageKind.StreamRequest:
      case SocketMessageKind.Event:
        if (!TryReserveFrame(frame))
        {
          RejectOverBudget(frame.Kind, frame.MessageId, creditTaken: true);
          return true;
        }

        // Message credits guarantee the queue has room; a full queue means the peer overran its credit.
        if (!TryEnqueueHandlerFrame(frame))
          throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Peer sent more messages than its credit allows.");
        return false;
      case SocketMessageKind.Response:
        DispatchResponse(frame);
        break;
      case SocketMessageKind.StreamStart:
        DispatchStreamStart(frame);
        break;
      case SocketMessageKind.StreamItem:
        DispatchStreamItem(frame);
        break;
      case SocketMessageKind.StreamEnd:
        DispatchStreamEnd(frame);
        break;
      case SocketMessageKind.Credit:
        DispatchCredit(frame);
        break;
      case SocketMessageKind.Ping:
        SendControl(new SocketFrame
        {
          MessageId = frame.MessageId,
          Kind = SocketMessageKind.Pong,
          Ok = true
        });
        break;
      case SocketMessageKind.Pong:
        break;
      case SocketMessageKind.Close:
        Lifetime.Cancel();
        break;
      case SocketMessageKind.Cancel:
        CancelActiveRequest(frame.MessageId);
        break;
      default:
        throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Unknown frame kind.");
    }

    return true;
  }

  async Task NotifyConnectedAsync()
  {
    try
    {
      ObservePeerConnected();
      await Handler.OnConnectedAsync(this, Lifetime.Token);
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      await CloseAsync(ex);
    }
  }

  async Task HeartbeatLoopAsync()
  {
    try
    {
      while (!Lifetime.IsCancellationRequested)
      {
        await Task.Delay(Options.HeartbeatInterval, Lifetime.Token);
        PruneExpiredIdempotentRequests();
        var idle = TimeSpan.FromMilliseconds(Environment.TickCount64 - Volatile.Read(ref LastReceivedTicks));
        if (idle > Options.IdleTimeout)
          throw new SocketProtocolException(SocketProtocolError.Timeout, "Socket peer idle timeout elapsed.");

        await WriteFrameAsync(new SocketFrame
        {
          MessageId = Interlocked.Increment(ref NextMessageId),
          Kind = SocketMessageKind.Ping
        }, Lifetime.Token);
      }
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      await CloseAsync(ex);
    }
  }

  // A cancel frame is best effort and never makes the caller wait: it already has its timeout or cancelled
  // result.
  void SendCancelInBackground(long messageId)
  {
    SendControl(new SocketFrame
    {
      MessageId = messageId,
      Kind = SocketMessageKind.Cancel
    });
  }

  // Server-side handler cancellation sources are never pooled and never disposed. The read loop can look
  // one up just as its request ends; with pooling, that late Cancel either hit a disposed source (faulting
  // the read loop and closing the connection) or cancelled an unrelated request that reused it. A source
  // that is simply dropped makes a late Cancel harmless: it only reaches a request that already finished.
  void CancelActiveRequest(long messageId)
  {
    if (!ActiveRequests.TryGetValue(messageId, out var active))
      return;

    active.CancelledByRemote = true;
    active.Source.Cancel();
  }

  static void EndActiveRequest(ActiveRequest active)
  {
    // Stops a pending deadline timer so it does not keep the source alive until the deadline.
    active.Source.CancelAfter(Timeout.InfiniteTimeSpan);
  }

  // One handler invocation on this side. CancelledByRemote tells a Cancel frame from the remote apart from
  // a deadline or idle timeout, which all cancel the same source.
  sealed class ActiveRequest
  {
    public CancellationTokenSource Source { get; } = new();
    public volatile bool CancelledByRemote;
  }
}
