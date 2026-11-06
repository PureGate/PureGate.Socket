using PureGate.Socket.Serialization;
using PureGate.Socket.Observability;
using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  // The payload is serialized straight into a pooled buffer, returned once the request has been sent.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  public ValueTask<SocketRequestResult> RequestAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return SendTypedRequestAsync(service, operation, payload, pooledResponse: false, cancellationToken);
  }

  // The response is deserialized here and its bytes never reach the caller, so they are read into a
  // pooled buffer and returned right after.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  public async ValueTask<TResponse> RequestAsync<TRequest, TResponse>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    var result = await SendTypedRequestAsync(service, operation, payload, pooledResponse: true, cancellationToken);
    return ReadTypedResult<TResponse>(result, Options.Codec);
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  async ValueTask<SocketRequestResult> SendTypedRequestAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      bool pooledResponse,
      CancellationToken cancellationToken)
  {
    var serialized = SocketPayloadWriter.Serialize(Options.Codec, payload);
    try
    {
      return await RequestPayloadAsync(service, operation, serialized, Options.RequestTimeout, idempotencyKey: null, cancellationToken, pooledResponse);
    }
    finally
    {
      serialized.Dispose();
    }
  }

  // Deserializes a typed result and releases its pooled payload, whatever happens.
  internal static TResponse ReadTypedResult<TResponse>(SocketRequestResult result, ISocketMessageCodec codec)
  {
    try
    {
      if (!result.Succeeded)
        throw new SocketProtocolException(result.ErrorCode, result.ErrorMessage);

      return result.ReadPayload<TResponse>(codec);
    }
    finally
    {
      result.ReleasePooled();
    }
  }

  public ValueTask<SocketRequestResult> RequestAsync(
      string service,
      string operation,
      byte[] payload,
      TimeSpan timeout,
      string idempotencyKey = null,
      CancellationToken cancellationToken = default)
  {
    return RequestPayloadAsync(service, operation, SocketPayload.Wrap(payload ?? []), timeout, idempotencyKey, cancellationToken);
  }

  // The caller keeps ownership of payload: it is no longer read once this completes, so a pooled payload
  // can be returned (or reused for a retry) right after.
  internal ValueTask<SocketRequestResult> RequestPayloadAsync(
      string service,
      string operation,
      SocketPayload payload,
      TimeSpan timeout,
      string idempotencyKey,
      CancellationToken cancellationToken,
      bool pooledResponse = false)
  {
    var frame = new SocketFrame
    {
      Kind = SocketMessageKind.Request,
      Service = service ?? string.Empty,
      Operation = operation ?? string.Empty,
      IdempotencyKey = idempotencyKey ?? string.Empty
    };
    frame.SetPayload(payload);
    return RequestFrameAsync(frame, timeout, cancellationToken, pooledResponse);
  }

  // Measures the request only when a metrics listener is attached; otherwise this adds nothing.
  ValueTask<SocketRequestResult> RequestFrameAsync(
      SocketFrame frame,
      TimeSpan timeout,
      CancellationToken cancellationToken,
      bool pooledResponse)
  {
    var started = SocketMetrics.ClientRequestStarted();
    return started == 0
        ? RequestFrameCoreAsync(frame, timeout, cancellationToken, pooledResponse)
        : MeasuredRequestFrameAsync(frame, timeout, cancellationToken, pooledResponse, started);
  }

  async ValueTask<SocketRequestResult> MeasuredRequestFrameAsync(
      SocketFrame frame,
      TimeSpan timeout,
      CancellationToken cancellationToken,
      bool pooledResponse,
      long started)
  {
    var result = await RequestFrameCoreAsync(frame, timeout, cancellationToken, pooledResponse);
    SocketMetrics.ClientRequestCompleted(frame.Service, frame.Operation, started, result.Succeeded ? null : result.ErrorCode);
    return result;
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  async ValueTask<SocketRequestResult> RequestFrameCoreAsync(
      SocketFrame frame,
      TimeSpan timeout,
      CancellationToken cancellationToken,
      bool pooledResponse)
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
    var pending = RentPendingRequest(messageId, pooledResponse);
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
      if (result.PayloadCompressed)
        result = DecompressResult(result);
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
    var serialized = SocketPayloadWriter.Serialize(Options.Codec, payload);
    try
    {
      return await OpenStreamPayloadAsync(service, operation, serialized, Options.RequestTimeout, cancellationToken);
    }
    finally
    {
      serialized.Dispose();
    }
  }

  public ValueTask<Stream> OpenStreamAsync(
      string service,
      string operation,
      byte[] payload,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    return OpenStreamPayloadAsync(service, operation, SocketPayload.Wrap(payload ?? []), timeout, cancellationToken);
  }

  // The caller keeps ownership of payload; it is no longer read once this completes.
  async ValueTask<Stream> OpenStreamPayloadAsync(
      string service,
      string operation,
      SocketPayload payload,
      TimeSpan timeout,
      CancellationToken cancellationToken)
  {
    ThrowIfClosed();
    if (!TryAcquireInFlight())
      throw new SocketProtocolException(SocketProtocolError.Overloaded, "Too many in-flight requests.");

    var messageId = Interlocked.Increment(ref NextMessageId);
    var stream = new SocketPullStream(
        Options.StreamWindowBytes,
        Options.MaxStreamWindowBytes,
        amount => SendCreditInBackground(messageId, amount),
        () => SendCancelInBackground(messageId),
        MemoryBudget.Release);
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
        DeadlineMs = ToDeadlineMilliseconds(effectiveTimeout)
      };
      request.SetPayload(payload);
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
    var serialized = SocketPayloadWriter.Serialize(Options.Codec, payload);
    try
    {
      await SendEventPayloadAsync(service, operation, serialized, cancellationToken);
    }
    finally
    {
      serialized.Dispose();
    }
  }

  public ValueTask SendEventAsync(
      string service,
      string operation,
      byte[] payload,
      CancellationToken cancellationToken = default)
  {
    return SendEventPayloadAsync(service, operation, SocketPayload.Wrap(payload ?? []), cancellationToken);
  }

  // The caller keeps ownership of payload; it is no longer read once this completes.
  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  internal async ValueTask SendEventPayloadAsync(
      string service,
      string operation,
      SocketPayload payload,
      CancellationToken cancellationToken)
  {
    ThrowIfClosed();
    var frame = new SocketFrame
    {
      MessageId = Interlocked.Increment(ref NextMessageId),
      Kind = SocketMessageKind.Event,
      Service = service ?? string.Empty,
      Operation = operation ?? string.Empty
    };
    frame.SetPayload(payload);

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

    SocketMetrics.EventSent(frame.Service, frame.Operation);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> RequestBatchAsync(
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var batch = ValidateBatch(items);
    SocketRequestResult result;
    using (var encoded = SocketBatchCodec.SerializeItems(batch))
      result = await RequestPayloadAsync(InternalService, BatchRequestOperation, encoded, timeout, idempotencyKey: null, cancellationToken);
    if (!result.Succeeded)
      throw new SocketProtocolException(result.ErrorCode, result.ErrorMessage);

    return SocketBatchCodec.DeserializeResults(result.Payload.Span);
  }

  public async ValueTask SendEventBatchAsync(
      IEnumerable<SocketBatchItem> items,
      CancellationToken cancellationToken = default)
  {
    var batch = ValidateBatch(items);
    using var encoded = SocketBatchCodec.SerializeItems(batch);
    await SendEventPayloadAsync(InternalService, BatchEventOperation, encoded, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> SendEventBatchWithResultsAsync(
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var batch = ValidateBatch(items);
    SocketRequestResult result;
    using (var encoded = SocketBatchCodec.SerializeItems(batch))
      result = await RequestPayloadAsync(InternalService, BatchEventRequestOperation, encoded, timeout, idempotencyKey: null, cancellationToken);
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
        await WaitForWritesAsync(closeTimeout.Token);
      }
      catch
      {
      }
    }

    await CloseAsync(null);
  }
}
