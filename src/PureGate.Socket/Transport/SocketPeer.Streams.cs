using System.Buffers;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  void DispatchStreamStart(SocketFrame frame)
  {
    if (!PendingStreams.TryGetValue(frame.MessageId, out var pending))
      return;

    if (frame.Ok)
    {
      pending.TrySetStarted();
      return;
    }

    PendingStreams.TryRemove(frame.MessageId, out _);
    pending.TrySetFailed(new SocketProtocolException(
        frame.ErrorCode ?? SocketProtocolError.HandlerFailed,
        frame.ErrorMessage ?? string.Empty));
    pending.Stream.Complete();
    pending.ReleaseInFlight(InFlightRequests);
  }

  async ValueTask DispatchStreamItemAsync(SocketFrame frame)
  {
    if (!PendingStreams.TryGetValue(frame.MessageId, out var pending))
    {
      frame.ReleasePayload();
      return;
    }

    await pending.Stream.WriteChunkAsync(
        frame.TakePayload(),
        Lifetime.Token);
  }

  void DispatchStreamEnd(SocketFrame frame)
  {
    if (!PendingStreams.TryRemove(frame.MessageId, out var pending))
      return;

    if (frame.Ok)
    {
      if (!pending.IsStarted)
        pending.TrySetStarted();
      pending.Stream.Complete();
    }
    else
    {
      var exception = new SocketProtocolException(
          frame.ErrorCode ?? SocketProtocolError.HandlerFailed,
          frame.ErrorMessage ?? string.Empty);
      if (!pending.IsStarted)
        pending.TrySetFailed(exception);
      pending.Stream.Complete(exception);
    }

    pending.ReleaseInFlight(InFlightRequests);
  }

  async ValueTask DispatchStreamRequestAsync(SocketFrame frame)
  {
    SocketResponse observedResponse = null;
    var requestTimeout = RentRequestCancellation();
    if (frame.DeadlineMs > 0)
      requestTimeout.CancelAfter(TimeSpan.FromMilliseconds(frame.DeadlineMs));

    ActiveRequestCancellations[frame.MessageId] = requestTimeout;
    try
    {
      ObserveRequestStarted(frame);
      var streamRequest = new SocketStreamRequest(
          frame.MessageId,
          frame.Service,
          frame.Operation,
          frame.PayloadMemory);
      var authorization = await Options.Authorizer.AuthorizeStreamAsync(this, streamRequest, requestTimeout.Token);
      if (!authorization.Succeeded)
      {
        observedResponse = SocketResponse.Failed(SocketProtocolError.Unauthorized, authorization.Error);
        await WriteStreamStartFailedAsync(frame.MessageId, SocketProtocolError.Unauthorized, authorization.Error, requestTimeout.Token);
        return;
      }

      var response = await Handler.HandleStreamAsync(this, streamRequest, requestTimeout.Token)
          ?? SocketStreamResponse.Failed(SocketProtocolError.UnsupportedOperation, "Stream request was not handled.");

      if (!response.Succeeded)
      {
        observedResponse = SocketResponse.Failed(response.ErrorCode, response.ErrorMessage);
        await WriteStreamStartFailedAsync(
            frame.MessageId,
            response.ErrorCode,
            response.ErrorMessage,
            requestTimeout.Token);
        return;
      }

      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = frame.MessageId,
        Kind = SocketMessageKind.StreamStart,
        Ok = true
      }, requestTimeout.Token);

      await WriteStreamItemsAsync(frame.MessageId, response.Stream ?? Stream.Null, requestTimeout.Token);
      observedResponse = SocketResponse.Success();
    }
    catch (OperationCanceledException) when (requestTimeout.IsCancellationRequested && !Lifetime.IsCancellationRequested)
    {
      observedResponse = SocketResponse.Failed(
          frame.DeadlineMs > 0 ? SocketProtocolError.Timeout : SocketProtocolError.Cancelled,
          frame.DeadlineMs > 0 ? "Stream deadline elapsed." : "Stream was cancelled.");
      await WriteStreamEndFailedAsync(
          frame.MessageId,
          observedResponse.ErrorCode,
          observedResponse.ErrorMessage);
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      observedResponse = SocketResponse.Failed(SocketProtocolError.HandlerFailed, ex.Message);
      await WriteStreamEndFailedAsync(frame.MessageId, observedResponse.ErrorCode, observedResponse.ErrorMessage);
    }
    finally
    {
      ObserveRequestCompleted(
          frame,
          observedResponse ?? SocketResponse.Failed(SocketProtocolError.Cancelled, "Stream did not complete."));
      ActiveRequestCancellations.TryRemove(frame.MessageId, out _);
      ReturnRequestCancellation(requestTimeout);
    }
  }

  async ValueTask WriteStreamItemsAsync(
      long messageId,
      Stream source,
      CancellationToken cancellationToken)
  {
    var buffer = ArrayPool<byte>.Shared.Rent(Options.StreamChunkBytes);
    try
    {
      await using (source)
      {
        while (true)
        {
          var read = await source.ReadAsync(buffer.AsMemory(0, Options.StreamChunkBytes), cancellationToken);
          if (read == 0)
            break;

          await WriteFrameAsync(new SocketFrame
          {
            Version = Options.ProtocolVersion,
            MessageId = messageId,
            Kind = SocketMessageKind.StreamItem
          }, buffer.AsMemory(0, read), cancellationToken);
        }
      }

      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = messageId,
        Kind = SocketMessageKind.StreamEnd,
        Ok = true
      }, cancellationToken);
    }
    finally
    {
      ArrayPool<byte>.Shared.Return(buffer);
    }
  }

  async ValueTask WriteStreamStartFailedAsync(
      long messageId,
      string errorCode,
      string errorMessage,
      CancellationToken cancellationToken)
  {
    await WriteFrameAsync(new SocketFrame
    {
      Version = Options.ProtocolVersion,
      MessageId = messageId,
      Kind = SocketMessageKind.StreamStart,
      Ok = false,
      ErrorCode = errorCode,
      ErrorMessage = errorMessage
    }, cancellationToken);
  }

  async ValueTask WriteStreamEndFailedAsync(
      long messageId,
      string errorCode,
      string errorMessage)
  {
    if (State == SocketPeerState.Closed || Lifetime.IsCancellationRequested)
      return;

    try
    {
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = messageId,
        Kind = SocketMessageKind.StreamEnd,
        Ok = false,
        ErrorCode = errorCode,
        ErrorMessage = errorMessage
      }, CancellationToken.None);
    }
    catch
    {
    }
  }
}
