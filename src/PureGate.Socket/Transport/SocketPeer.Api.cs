using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  public async ValueTask<SocketRequestResult> RequestAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await RequestAsync(
        service,
        operation,
        Options.Codec.Serialize(payload),
        Options.RequestTimeout,
        idempotencyKey: null,
        cancellationToken);
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  public async ValueTask<TResponse> RequestAsync<TRequest, TResponse>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    var result = await RequestAsync(service, operation, payload, cancellationToken);
    if (!result.Succeeded)
      throw new SocketProtocolException(result.ErrorCode, result.ErrorMessage);

    return result.ReadPayload<TResponse>(Options.Codec);
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  public async ValueTask<SocketRequestResult> RequestAsync(
      string service,
      string operation,
      byte[] payload,
      TimeSpan timeout,
      string idempotencyKey = null,
      CancellationToken cancellationToken = default)
  {
    return await RequestFrameAsync(new SocketFrame
    {
      Kind = SocketMessageKind.Request,
      Service = service ?? string.Empty,
      Operation = operation ?? string.Empty,
      IdempotencyKey = idempotencyKey ?? string.Empty,
      Payload = payload ?? []
    }, timeout, cancellationToken);
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  async ValueTask<SocketRequestResult> RequestFrameAsync(
      SocketFrame frame,
      TimeSpan timeout,
      CancellationToken cancellationToken)
  {
    ThrowIfClosed();
    if (!TryAcquireInFlight())
    {
      return new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.Overloaded,
        ErrorMessage = "Too many in-flight requests."
      };
    }

    var messageId = Interlocked.Increment(ref NextMessageId);
    var pending = RentPendingRequest(messageId);
    var sentTicks = Environment.TickCount64;
    var effectiveTimeout = timeout <= TimeSpan.Zero ? Options.RequestTimeout : timeout;
    var requestTimeout = RentRequestCancellation();
    requestTimeout.CancelAfter(effectiveTimeout);
    CancellationTokenSource writeCancellation = null;
    CancellationTokenRegistration timeoutRegistration = default;
    CancellationTokenRegistration cancellationRegistration = default;
    var writeToken = cancellationToken.CanBeCanceled
        ? (writeCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, requestTimeout.Token, Lifetime.Token)).Token
        : requestTimeout.Token;

    try
    {
      if (!PendingRequests.TryAdd(messageId, pending))
        throw new InvalidOperationException("Could not register socket request.");

      timeoutRegistration = requestTimeout.Token.UnsafeRegister(
          static state => ((PendingRequest)state).TrySetTimeout(),
          pending);
      if (cancellationToken.CanBeCanceled)
      {
        cancellationRegistration = cancellationToken.UnsafeRegister(
            static state => ((PendingRequest)state).TrySetCancelled(),
            pending);
      }

      frame.MessageId = messageId;
      frame.DeadlineMs = ToDeadlineMilliseconds(effectiveTimeout);

      // Waits while the remote's handler queue is full; the request's own timeout still applies.
      await MessageCredits.AcquireAsync(writeToken);
      try
      {
        await WriteFrameAsync(frame, writeToken);
      }
      catch
      {
        RefundUnsentMessage(frame);
        throw;
      }

      var result = await pending.Task;
      if (result.ErrorCode is SocketProtocolError.Timeout or SocketProtocolError.Cancelled)
        SendCancelInBackground(messageId);
      if (result.ErrorCode == SocketProtocolError.Timeout)
        _ = CheckLivenessAsync(sentTicks);

      return result;
    }
    catch (OperationCanceledException) when (requestTimeout.IsCancellationRequested)
    {
      var result = cancellationToken.IsCancellationRequested
          ? new SocketRequestResult
          {
            Succeeded = false,
            ErrorCode = SocketProtocolError.Cancelled,
            ErrorMessage = "Request was cancelled."
          }
          : new SocketRequestResult
          {
            Succeeded = false,
            ErrorCode = SocketProtocolError.Timeout,
            ErrorMessage = "Request timed out."
          };
      SendCancelInBackground(messageId);
      if (result.ErrorCode == SocketProtocolError.Timeout)
        _ = CheckLivenessAsync(sentTicks);
      return result;
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      SendCancelInBackground(messageId);
      return new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.Cancelled,
        ErrorMessage = "Request was cancelled."
      };
    }
    catch (OperationCanceledException) when (Lifetime.IsCancellationRequested)
    {
      return new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.ConnectionClosed,
        ErrorMessage = "Connection closed."
      };
    }
    catch (SocketProtocolException ex) when (ex.ErrorCode == SocketProtocolError.ConnectionClosed)
    {
      return new SocketRequestResult
      {
        Succeeded = false,
        ErrorCode = SocketProtocolError.ConnectionClosed,
        ErrorMessage = "Connection closed."
      };
    }
    finally
    {
      timeoutRegistration.Dispose();
      cancellationRegistration.Dispose();
      writeCancellation?.Dispose();
      ReturnRequestCancellation(requestTimeout);
      PendingRequests.TryRemove(messageId, out _);
      ReleaseInFlight();
      ReturnPendingRequest(pending);
    }
  }

  public async ValueTask<Stream> OpenStreamAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await OpenStreamAsync(
        service,
        operation,
        Options.Codec.Serialize(payload),
        Options.RequestTimeout,
        cancellationToken);
  }

  public async ValueTask<Stream> OpenStreamAsync(
      string service,
      string operation,
      byte[] payload,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    ThrowIfClosed();
    if (!TryAcquireInFlight())
      throw new SocketProtocolException(SocketProtocolError.Overloaded, "Too many in-flight requests.");

    var messageId = Interlocked.Increment(ref NextMessageId);
    var stream = new SocketPullStream(
        Options.StreamWindowBytes,
        Options.MaxStreamWindowBytes,
        amount => SendCreditInBackground(messageId, amount),
        () => SendCancelInBackground(messageId));
    var pending = new PendingStream(stream);
    var effectiveTimeout = timeout <= TimeSpan.Zero ? Options.RequestTimeout : timeout;
    using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Lifetime.Token);
    timeoutSource.CancelAfter(effectiveTimeout);
    var registered = false;

    try
    {
      if (!PendingStreams.TryAdd(messageId, pending))
        throw new InvalidOperationException("Could not register socket stream request.");
      registered = true;

      // The value carries the initial stream window: how many bytes the producer may send before the
      // first grant.
      var request = new SocketFrame
      {
        MessageId = messageId,
        Kind = SocketMessageKind.StreamRequest,
        Service = service ?? string.Empty,
        Operation = operation ?? string.Empty,
        Value = Options.StreamWindowBytes,
        DeadlineMs = ToDeadlineMilliseconds(effectiveTimeout),
        Payload = payload ?? []
      };
      await MessageCredits.AcquireAsync(timeoutSource.Token);
      try
      {
        await WriteFrameAsync(request, timeoutSource.Token);
      }
      catch
      {
        RefundUnsentMessage(request);
        throw;
      }

      using var registration = timeoutSource.Token.Register(
          static state => ((PendingStream)state).TrySetCancelled(),
          pending);
      return await pending.Task;
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      if (registered && PendingStreams.TryRemove(messageId, out _))
        pending.ReleaseInFlight(this);
      if (!registered)
        pending.ReleaseInFlight(this);
      stream.Complete();
      SendCancelInBackground(messageId);
      throw new SocketProtocolException(SocketProtocolError.Cancelled, "Stream request was cancelled.");
    }
    catch (OperationCanceledException)
    {
      if (registered && PendingStreams.TryRemove(messageId, out _))
        pending.ReleaseInFlight(this);
      if (!registered)
        pending.ReleaseInFlight(this);
      stream.Complete();
      SendCancelInBackground(messageId);
      throw new SocketProtocolException(SocketProtocolError.Timeout, "Stream request timed out.");
    }
    catch
    {
      if (registered && PendingStreams.TryRemove(messageId, out _))
        pending.ReleaseInFlight(this);
      if (!registered)
        pending.ReleaseInFlight(this);
      stream.Complete();
      throw;
    }
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  public async ValueTask SendEventAsync<TPayload>(
      string service,
      string operation,
      TPayload payload,
      CancellationToken cancellationToken = default)
  {
    await SendEventAsync(service, operation, Options.Codec.Serialize(payload), cancellationToken);
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  public async ValueTask SendEventAsync(
      string service,
      string operation,
      byte[] payload,
      CancellationToken cancellationToken = default)
  {
    ThrowIfClosed();
    var frame = new SocketFrame
    {
      MessageId = Interlocked.Increment(ref NextMessageId),
      Kind = SocketMessageKind.Event,
      Service = service ?? string.Empty,
      Operation = operation ?? string.Empty,
      Payload = payload ?? []
    };

    // Waits while the remote's handler queue is full, instead of the remote dropping the connection.
    try
    {
      await MessageCredits.AcquireAsync(cancellationToken);
    }
    catch (OperationCanceledException) when (Lifetime.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
    {
      throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, "Connection is closed.");
    }

    try
    {
      await WriteFrameAsync(frame, cancellationToken);
    }
    catch
    {
      RefundUnsentMessage(frame);
      throw;
    }
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> RequestBatchAsync(
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var batch = ValidateBatch(items);
    var result = await RequestAsync(
        InternalService,
        BatchRequestOperation,
        SocketBatchCodec.SerializeItems(batch),
        timeout,
        idempotencyKey: null,
        cancellationToken);
    if (!result.Succeeded)
      throw new SocketProtocolException(result.ErrorCode, result.ErrorMessage);

    return SocketBatchCodec.DeserializeResults(result.Payload.Span);
  }

  public async ValueTask SendEventBatchAsync(
      IEnumerable<SocketBatchItem> items,
      CancellationToken cancellationToken = default)
  {
    var batch = ValidateBatch(items);
    await SendEventAsync(
        InternalService,
        BatchEventOperation,
        SocketBatchCodec.SerializeItems(batch),
        cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> SendEventBatchWithResultsAsync(
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var batch = ValidateBatch(items);
    var result = await RequestAsync(
        InternalService,
        BatchEventRequestOperation,
        SocketBatchCodec.SerializeItems(batch),
        timeout,
        idempotencyKey: null,
        cancellationToken);
    if (!result.Succeeded)
      throw new SocketProtocolException(result.ErrorCode, result.ErrorMessage);

    return SocketBatchCodec.DeserializeResults(result.Payload.Span);
  }

  static readonly TimeSpan CloseFrameTimeout = TimeSpan.FromSeconds(2);

  public async ValueTask CloseGracefullyAsync(CancellationToken cancellationToken = default)
  {
    if (State != SocketPeerState.Closed && !Lifetime.IsCancellationRequested)
    {
      try
      {
        // The close frame is a courtesy. If the write side is blocked (for example a peer that stopped
        // reading), give up after a short wait; closing the socket below unblocks every pending writer.
        using var closeTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        closeTimeout.CancelAfter(CloseFrameTimeout);
        await WriteFrameAsync(new SocketFrame
        {
          Kind = SocketMessageKind.Close
        }, closeTimeout.Token);
      }
      catch
      {
      }
    }

    await CloseAsync(null);
  }
}
