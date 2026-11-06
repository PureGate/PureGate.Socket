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

        LastReceivedTicks = DateTimeOffset.UtcNow.UtcTicks;
        ObserveFrameReceived(frame);
        PruneExpiredChunks();
        if (frame.Kind == SocketMessageKind.Chunk)
        {
          frame = ReassembleChunk(frame);
          if (frame is null)
            continue;
        }

        var releasePayload = true;
        try
        {
          releasePayload = await DispatchAsync(frame);
        }
        finally
        {
          if (releasePayload)
            frame.ReleasePayload();
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

  async ValueTask<bool> DispatchAsync(SocketFrame frame)
  {
    if (frame.Version != Options.ProtocolVersion)
      throw new SocketProtocolException(SocketProtocolError.UnsupportedVersion, "Unsupported protocol version.");

    switch (frame.Kind)
    {
      case SocketMessageKind.Request:
      case SocketMessageKind.FastRequest:
      case SocketMessageKind.StreamRequest:
        return await EnqueueHandlerFrameAsync(frame);
      case SocketMessageKind.Response:
      case SocketMessageKind.FastResponse:
        DispatchResponse(frame);
        break;
      case SocketMessageKind.StreamStart:
        DispatchStreamStart(frame);
        break;
      case SocketMessageKind.StreamItem:
        await DispatchStreamItemAsync(frame);
        break;
      case SocketMessageKind.StreamEnd:
        DispatchStreamEnd(frame);
        break;
      case SocketMessageKind.Event:
        return await EnqueueHandlerFrameAsync(frame);
      case SocketMessageKind.Ping:
        await WriteFrameAsync(new SocketFrame
        {
          Version = Options.ProtocolVersion,
          MessageId = frame.MessageId,
          Kind = SocketMessageKind.Pong,
          Ok = true
        }, Lifetime.Token);
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

  async ValueTask<bool> EnqueueHandlerFrameAsync(SocketFrame frame)
  {
    if (HandlerQueue.Writer.TryWrite(frame))
      return false;

    // The request was rejected before any handler ran, so the caller can safely retry it.
    if (frame.Kind is SocketMessageKind.Request or SocketMessageKind.FastRequest)
    {
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = frame.MessageId,
        Kind = frame.Kind == SocketMessageKind.FastRequest ? SocketMessageKind.FastResponse : SocketMessageKind.Response,
        OperationId = frame.OperationId,
        Ok = false,
        ErrorCode = SocketProtocolError.Overloaded,
        ErrorMessage = "Handler queue is full."
      }, Lifetime.Token);
      return true;
    }

    if (frame.Kind == SocketMessageKind.StreamRequest)
    {
      await WriteStreamStartFailedAsync(
          frame.MessageId,
          SocketProtocolError.Overloaded,
          "Handler queue is full.",
          Lifetime.Token);
      return true;
    }

    // Events cannot be rejected individually yet; per-connection flow control arrives in Phase 2.
    throw new SocketProtocolException(SocketProtocolError.Overloaded, "Handler queue is full.");
  }

  async Task HandlerLoopAsync()
  {
    try
    {
      await foreach (var frame in HandlerQueue.Reader.ReadAllAsync(Lifetime.Token))
      {
        try
        {
          switch (frame.Kind)
          {
            case SocketMessageKind.Request:
              await DispatchRequestAsync(frame);
              break;
            case SocketMessageKind.FastRequest:
              await DispatchFastRequestAsync(frame);
              break;
            case SocketMessageKind.StreamRequest:
              await DispatchStreamRequestAsync(frame);
              break;
            case SocketMessageKind.Event:
              await DispatchEventAsync(frame);
              break;
          }
        }
        catch (Exception ex) when (!Lifetime.IsCancellationRequested)
        {
          // Dispatch methods turn every handler failure into a response, so what reaches this point is a
          // transport failure while answering. The connection is unusable; close it.
          await CloseAsync(ex);
        }
        catch
        {
          // The connection is closing; there is nobody left to answer.
        }
        finally
        {
          frame.ReleasePayload();
        }
      }
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
        var idle = DateTimeOffset.UtcNow - new DateTimeOffset(Interlocked.Read(ref LastReceivedTicks), TimeSpan.Zero);
        if (idle > Options.IdleTimeout)
          throw new SocketProtocolException(SocketProtocolError.Timeout, "Socket peer idle timeout elapsed.");

        await WriteFrameAsync(new SocketFrame
        {
          Version = Options.ProtocolVersion,
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

  static readonly TimeSpan CancelFrameTimeout = TimeSpan.FromSeconds(5);

  // A cancel frame is best effort: the caller already has its timeout or cancelled result and should not
  // wait behind a write lock held by a blocked write.
  void SendCancelInBackground(long messageId)
  {
    _ = SendCancelAsync(messageId).AsTask();
  }

  async ValueTask SendCancelAsync(long messageId)
  {
    if (State == SocketPeerState.Closed || Lifetime.IsCancellationRequested)
      return;

    try
    {
      using var timeout = new CancellationTokenSource(CancelFrameTimeout);
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = messageId,
        Kind = SocketMessageKind.Cancel
      }, timeout.Token);
    }
    catch
    {
    }
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
