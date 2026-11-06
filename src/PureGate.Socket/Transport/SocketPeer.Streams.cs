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
    pending.ReleaseInFlight(this);
  }

  // Never waits: the window bounds what the producer may send, so the chunk is buffered as it is.
  void DispatchStreamItem(SocketFrame frame)
  {
    if (!PendingStreams.TryGetValue(frame.MessageId, out var pending))
    {
      frame.ReleasePayload();
      return;
    }

    var producerWaiting = (frame.Flags & SocketFrameFlags.WindowExhausted) != 0;
    if (!pending.Stream.TryWriteChunk(frame.TakePayload(), producerWaiting))
      throw new SocketProtocolException(SocketProtocolError.ProtocolError, "Stream data exceeds the granted window.");
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

    pending.ReleaseInFlight(this);
  }

  async ValueTask DispatchStreamRequestAsync(SocketFrame frame)
  {
    SocketResponse observedResponse = null;
    Stream source = null;
    var started = false;
    var active = new ActiveRequest();
    var requestTimeout = active.Source;
    if (frame.DeadlineMs > 0)
      requestTimeout.CancelAfter(TimeSpan.FromMilliseconds(frame.DeadlineMs));

    ActiveRequests[frame.MessageId] = active;
    try
    {
      try
      {
        ObserveRequestStarted(frame);

        // OperationId carries the consumer's initial window.
        if (frame.OperationId <= 0)
        {
          observedResponse = SocketResponse.Failed(SocketProtocolError.ProtocolError, "Stream request carries no flow-control window.");
          await WriteStreamStartFailedAsync(frame.MessageId, observedResponse.ErrorCode, observedResponse.ErrorMessage, requestTimeout.Token);
          return;
        }

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
        source = response.Stream;

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

        // Registered before StreamStart so no grant from the consumer can arrive before the window exists.
        var window = new StreamSendWindow(frame.OperationId);
        OutgoingStreams[frame.MessageId] = window;
        await WriteFrameAsync(new SocketFrame
        {
          Version = Options.ProtocolVersion,
          MessageId = frame.MessageId,
          Kind = SocketMessageKind.StreamStart,
          Ok = true
        }, requestTimeout.Token);

        // The caller's deadline covered opening the stream. From here on the stream may run as long as it
        // keeps making progress: the idle timer is re-armed after every chunk, and also bounds how long
        // the producer waits for the consumer to grant more window.
        started = true;
        requestTimeout.CancelAfter(Options.StreamIdleTimeout);
        await WriteStreamItemsAsync(frame.MessageId, source ?? Stream.Null, window, requestTimeout);
        observedResponse = SocketResponse.Success();
      }
      catch (OperationCanceledException) when (requestTimeout.IsCancellationRequested && !Lifetime.IsCancellationRequested)
      {
        observedResponse = active.CancelledByRemote
            ? SocketResponse.Failed(SocketProtocolError.Cancelled, "Stream was cancelled.")
            : started
                ? SocketResponse.Failed(SocketProtocolError.Timeout, "Stream idle timeout elapsed.")
                : frame.DeadlineMs > 0
                    ? SocketResponse.Failed(SocketProtocolError.Timeout, "Stream deadline elapsed.")
                    : SocketResponse.Failed(SocketProtocolError.Cancelled, "Stream was cancelled.");
        await WriteStreamEndFailedAsync(
            frame.MessageId,
            observedResponse.ErrorCode,
            observedResponse.ErrorMessage);
      }
      catch (Exception) when (Lifetime.IsCancellationRequested)
      {
      }
      catch (Exception ex)
      {
        observedResponse = HandlerFailure(frame.Service, frame.Operation, ex);
        await WriteStreamEndFailedAsync(frame.MessageId, observedResponse.ErrorCode, observedResponse.ErrorMessage);
      }
    }
    finally
    {
      await DisposeStreamSourceAsync(source);
      ObserveRequestCompleted(
          frame,
          observedResponse ?? SocketResponse.Failed(SocketProtocolError.Cancelled, "Stream did not complete."));
      OutgoingStreams.TryRemove(frame.MessageId, out _);
      ActiveRequests.TryRemove(frame.MessageId, out _);
      EndActiveRequest(active);
    }
  }

  static async ValueTask DisposeStreamSourceAsync(Stream source)
  {
    if (source is null)
      return;

    try
    {
      await source.DisposeAsync();
    }
    catch
    {
    }
  }

  async ValueTask WriteStreamItemsAsync(
      long messageId,
      Stream source,
      StreamSendWindow window,
      CancellationTokenSource idleTimeout)
  {
    var cancellationToken = idleTimeout.Token;
    var buffer = ArrayPool<byte>.Shared.Rent(Options.StreamChunkBytes);
    try
    {
      while (true)
      {
        var read = await source.ReadAsync(buffer.AsMemory(0, Options.StreamChunkBytes), cancellationToken);
        if (read == 0)
          break;

        // Send what the window allows, then wait for the consumer to grant more.
        for (var offset = 0; offset < read;)
        {
          var allowed = await window.TakeAsync(read - offset, cancellationToken);

          // Telling the consumer that this frame used up the window lets it grow the window when it then
          // runs dry (both sides waiting on each other means the window is the bottleneck).
          await WriteFrameAsync(new SocketFrame
          {
            Version = Options.ProtocolVersion,
            MessageId = messageId,
            Kind = SocketMessageKind.StreamItem,
            Flags = window.IsExhausted ? SocketFrameFlags.WindowExhausted : 0
          }, buffer.AsMemory(offset, allowed), cancellationToken);
          offset += allowed;
          idleTimeout.CancelAfter(Options.StreamIdleTimeout);
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
