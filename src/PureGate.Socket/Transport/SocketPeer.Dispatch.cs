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
        if (frame.Kind == SocketMessageKind.Chunk)
        {
          PruneExpiredChunks();
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

    if (frame.Kind is SocketMessageKind.Request or SocketMessageKind.FastRequest)
    {
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = frame.MessageId,
        Kind = frame.Kind == SocketMessageKind.FastRequest ? SocketMessageKind.FastResponse : SocketMessageKind.Response,
        OperationId = frame.OperationId,
        Ok = false,
        ErrorCode = SocketProtocolError.ProtocolError,
        ErrorMessage = "Handler queue is full."
      }, Lifetime.Token);
      return true;
    }

    if (frame.Kind == SocketMessageKind.StreamRequest)
    {
      await WriteStreamStartFailedAsync(
          frame.MessageId,
          SocketProtocolError.ProtocolError,
          "Handler queue is full.",
          Lifetime.Token);
      return true;
    }

    throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Handler queue is full.");
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
        PruneExpiredChunks();
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
  async ValueTask SendCancelAsync(long messageId)
  {
    if (State == SocketPeerState.Closed || Lifetime.IsCancellationRequested)
      return;

    try
    {
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = messageId,
        Kind = SocketMessageKind.Cancel
      }, CancellationToken.None);
    }
    catch
    {
    }
  }

  void CancelActiveRequest(long messageId)
  {
    if (ActiveRequestCancellations.TryGetValue(messageId, out var cancellation))
      cancellation.Cancel();
  }
}
