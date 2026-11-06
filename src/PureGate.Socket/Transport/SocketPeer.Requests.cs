using System.Runtime.CompilerServices;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  // Error handling contract for every dispatch path below:
  // - Cancellation caused by the request's own deadline or a Cancel frame becomes a timeout/cancelled response.
  // - Anything else a handler throws (including an OperationCanceledException it raised itself) becomes a
  //   handler_failed response and is reported to the observer. It never escapes into the worker loop.
  // - Once the connection is closing, nothing is sent and nothing is reported.

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask DispatchRequestAsync(SocketFrame frame)
  {
    SocketResponse response;
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
        var request = new SocketRequest(
            frame.MessageId,
            frame.Service,
            frame.Operation,
            frame.PayloadMemory,
            frame.IdempotencyKey);
        response = string.IsNullOrEmpty(request.IdempotencyKey)
            ? await HandleRequestAsync(request, requestTimeout.Token)
            : await HandleIdempotentRequestAsync(request, requestTimeout.Token);
      }
      catch (OperationCanceledException) when (requestTimeout.IsCancellationRequested && !Lifetime.IsCancellationRequested)
      {
        response = DeadlineOrCancelled(frame, active);
      }
      catch (Exception) when (Lifetime.IsCancellationRequested)
      {
        return;
      }
      catch (Exception ex)
      {
        response = HandlerFailure(frame.Service, frame.Operation, ex);
      }

      response ??= SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Request was not handled.");
      ObserveRequestCompleted(frame, response);
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = frame.MessageId,
        Kind = SocketMessageKind.Response,
        Ok = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage
      }, response.Payload, Lifetime.Token);
    }
    finally
    {
      ActiveRequests.TryRemove(frame.MessageId, out _);
      EndActiveRequest(active);
    }
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask DispatchFastRequestAsync(SocketFrame frame)
  {
    SocketResponse response;
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
        var request = new SocketFastRequest(
            frame.MessageId,
            frame.OperationId,
            frame.PayloadMemory);
        response = request.OperationId == InternalFastBatchOperationId
            ? await HandleFastBatchRequestAsync(request, requestTimeout.Token)
            : await HandleFastRequestAsync(request, requestTimeout.Token);
      }
      catch (OperationCanceledException) when (requestTimeout.IsCancellationRequested && !Lifetime.IsCancellationRequested)
      {
        response = DeadlineOrCancelled(frame, active);
      }
      catch (Exception) when (Lifetime.IsCancellationRequested)
      {
        return;
      }
      catch (Exception ex)
      {
        response = HandlerFailure(string.Empty, FastOperationName(frame.OperationId), ex);
      }

      response ??= SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Fast request was not handled.");
      ObserveRequestCompleted(frame, response);
      await WriteFrameAsync(new SocketFrame
      {
        Version = Options.ProtocolVersion,
        MessageId = frame.MessageId,
        Kind = SocketMessageKind.FastResponse,
        OperationId = frame.OperationId,
        Ok = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage
      }, response.Payload, Lifetime.Token);
    }
    finally
    {
      ActiveRequests.TryRemove(frame.MessageId, out _);
      EndActiveRequest(active);
    }
  }

  static SocketResponse DeadlineOrCancelled(SocketFrame frame, ActiveRequest active)
  {
    return frame.DeadlineMs > 0 && !active.CancelledByRemote
        ? SocketResponse.Failed(SocketProtocolError.Timeout, "Request deadline elapsed.")
        : SocketResponse.Failed(SocketProtocolError.Cancelled, "Request was cancelled.");
  }

  // Fast requests have no service or operation names, so failures are reported under the operation ID.
  static string FastOperationName(int operationId)
  {
    return operationId.ToString(System.Globalization.CultureInfo.InvariantCulture);
  }

  // Turns a handler exception into the response the remote peer sees. A SocketProtocolException is a
  // deliberate, wire-safe error, so its code and message are kept. Any other exception is reported to the
  // observer in full and sent as a generic message unless IncludeExceptionDetails is set.
  SocketResponse HandlerFailure(string service, string operation, Exception exception)
  {
    ObserveHandlerFailed(service, operation, exception);
    if (exception is SocketProtocolException protocolException)
      return SocketResponse.Failed(protocolException.ErrorCode, protocolException.Message);

    return SocketResponse.Failed(
        SocketProtocolError.HandlerFailed,
        Options.IncludeExceptionDetails ? exception.Message : "The remote handler failed.");
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  async ValueTask<SocketResponse> HandleFastRequestAsync(
      SocketFastRequest request,
      CancellationToken cancellationToken)
  {
    var authorization = await Options.Authorizer.AuthorizeFastRequestAsync(this, request, cancellationToken);
    if (!authorization.Succeeded)
      return SocketResponse.Failed(SocketProtocolError.Unauthorized, authorization.Error);

    return await Handler.HandleFastRequestAsync(this, request, cancellationToken);
  }

  async ValueTask<SocketResponse> HandleFastBatchRequestAsync(
      SocketFastRequest batchRequest,
      CancellationToken cancellationToken)
  {
    var count = SocketFastBatchCodec.ReadItemCount(batchRequest.Payload, out var offset);
    var results = new SocketFastBatchCodec.ResultWriter(count);
    for (var i = 0; i < count; i++)
    {
      var item = SocketFastBatchCodec.ReadItem(batchRequest.Payload, ref offset);
      await HandleFastBatchRequestItemAsync(item, results, cancellationToken);
    }

    SocketFastBatchCodec.EnsureConsumed(batchRequest.Payload, offset);

    return new SocketResponse
    {
      Succeeded = true,
      Payload = results.ToArray()
    };
  }

  async ValueTask HandleFastBatchRequestItemAsync(
      SocketFastBatchItemView item,
      SocketFastBatchCodec.ResultWriter results,
      CancellationToken cancellationToken)
  {
    try
    {
      if (item.OperationId == InternalFastBatchOperationId)
      {
        results.WriteResult(
            item.Id,
            false,
            ReadOnlyMemory<byte>.Empty,
            SocketProtocolError.ProtocolError,
            "Nested fast batch messages are not supported.");
        return;
      }

      var request = new SocketFastRequest(item.Id, item.OperationId, item.Payload);
      var response = await HandleFastRequestAsync(request, cancellationToken)
          ?? SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Fast request was not handled.");
      results.WriteResult(
          item.Id,
          response.Succeeded,
          response.Payload,
          response.ErrorCode,
          response.ErrorMessage);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      throw;
    }
    catch (Exception ex)
    {
      var failure = HandlerFailure(string.Empty, FastOperationName(item.OperationId), ex);
      results.WriteResult(
          item.Id,
          false,
          ReadOnlyMemory<byte>.Empty,
          failure.ErrorCode,
          failure.ErrorMessage);
    }
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
  async ValueTask DispatchEventAsync(SocketFrame frame)
  {
    try
    {
      var socketEvent = new SocketEvent(frame.Service, frame.Operation, frame.PayloadMemory);
      if (IsBatchEvent(socketEvent))
      {
        await HandleBatchEventAsync(socketEvent, Lifetime.Token);
        return;
      }

      var authorization = await Options.Authorizer.AuthorizeEventAsync(this, socketEvent, Lifetime.Token);
      if (!authorization.Succeeded)
      {
        if (Options.CloseOnUnauthorizedEvent)
          await CloseAsync(new SocketProtocolException(SocketProtocolError.Unauthorized, authorization.Error));
        return;
      }

      await Handler.HandleEventAsync(
          this,
          socketEvent,
          Lifetime.Token);
    }
    catch (Exception) when (Lifetime.IsCancellationRequested)
    {
    }
    catch (Exception ex)
    {
      // Events have no response, so a failing handler is reported and the connection stays open.
      ObserveHandlerFailed(frame.Service, frame.Operation, ex);
    }
  }

  void DispatchResponse(SocketFrame frame)
  {
    if (!PendingRequests.TryGetValue(frame.MessageId, out var pending))
      return;

    pending.TrySetResult(frame.MessageId, new SocketRequestResult
    {
      Succeeded = frame.Ok,
      Payload = frame.PayloadMemory,
      ErrorCode = frame.ErrorCode,
      ErrorMessage = frame.ErrorMessage
    });
  }

  async ValueTask<SocketResponse> HandleBatchRequestAsync(
      SocketRequest batchRequest,
      CancellationToken cancellationToken)
  {
    var items = SocketBatchCodec.DeserializeItems(batchRequest.Payload.Span);
    var results = new SocketBatchResult[items.Count];
    for (var i = 0; i < items.Count; i++)
    {
      results[i] = string.Equals(batchRequest.Operation, BatchEventRequestOperation, StringComparison.Ordinal)
          ? await HandleBatchEventItemAsync(items[i], cancellationToken)
          : await HandleBatchRequestItemAsync(items[i], cancellationToken);
    }

    return new SocketResponse
    {
      Succeeded = true,
      Payload = SocketBatchCodec.SerializeResults(results)
    };
  }

  [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
  async ValueTask<SocketResponse> HandleRequestAsync(
      SocketRequest request,
      CancellationToken cancellationToken)
  {
    if (IsBatchRequest(request))
      return await HandleBatchRequestAsync(request, cancellationToken);

    var authorization = await Options.Authorizer.AuthorizeRequestAsync(this, request, cancellationToken);
    if (!authorization.Succeeded)
      return SocketResponse.Failed(SocketProtocolError.Unauthorized, authorization.Error);

    return await Handler.HandleRequestAsync(this, request, cancellationToken);
  }

  async ValueTask<SocketResponse> HandleIdempotentRequestAsync(
      SocketRequest request,
      CancellationToken cancellationToken)
  {
    PruneExpiredIdempotentRequests();
    var key = new SocketIdempotencyKey(
        RemoteNodeId,
        request.Service,
        request.Operation,
        request.IdempotencyKey);

    while (true)
    {
      var candidate = new SocketIdempotencyEntry();
      var entry = Options.IdempotencyCache.GetOrAdd(key, candidate);
      if (!ReferenceEquals(entry, candidate))
      {
        try
        {
          return await entry.Task.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
          // The attempt that owned this key was cancelled (typically because its connection dropped)
          // before it produced a response. Its entry is gone, so this attempt takes over.
          continue;
        }
      }

      try
      {
        var response = await HandleRequestAsync(request, cancellationToken)
            ?? SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Request was not handled.");
        candidate.TrySetResult(response);
        return response;
      }
      catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
      {
        Options.IdempotencyCache.Remove(key, candidate);
        candidate.TrySetCanceled();
        throw;
      }
      catch (Exception ex)
      {
        var response = HandlerFailure(request.Service, request.Operation, ex);
        candidate.TrySetResult(response);
        return response;
      }
    }
  }

  async ValueTask<SocketBatchResult> HandleBatchRequestItemAsync(
      SocketBatchItem item,
      CancellationToken cancellationToken)
  {
    try
    {
      if (IsInternalBatch(item.Service, item.Operation))
        return BatchFailed(item.Id, SocketProtocolError.ProtocolError, "Nested batch messages are not supported.");

      var request = new SocketRequest(item.Id, item.Service, item.Operation, item.Payload ?? []);
      var authorization = await Options.Authorizer.AuthorizeRequestAsync(this, request, cancellationToken);
      if (!authorization.Succeeded)
        return BatchFailed(item.Id, SocketProtocolError.Unauthorized, authorization.Error);

      var response = await Handler.HandleRequestAsync(this, request, cancellationToken)
          ?? SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Request was not handled.");
      return new SocketBatchResult(
          item.Id,
          response.Succeeded,
          response.Payload.ToArray(),
          response.ErrorCode,
          response.ErrorMessage);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      throw;
    }
    catch (Exception ex)
    {
      var failure = HandlerFailure(item.Service, item.Operation, ex);
      return BatchFailed(item.Id, failure.ErrorCode, failure.ErrorMessage);
    }
  }

  async ValueTask HandleBatchEventAsync(
      SocketEvent batchEvent,
      CancellationToken cancellationToken)
  {
    IReadOnlyList<SocketBatchItem> items;
    try
    {
      items = SocketBatchCodec.DeserializeItems(batchEvent.Payload.Span);
    }
    catch (SocketProtocolException ex)
    {
      // A malformed internal envelope is a protocol violation, not a handler failure.
      await CloseAsync(ex);
      return;
    }

    // Only the library's own checks decide whether the connection closes. A handler failure, whatever error
    // code it carries, is reported and the batch continues, exactly as for a single event.
    foreach (var item in items)
    {
      if (IsInternalBatch(item.Service, item.Operation))
      {
        await CloseAsync(new SocketProtocolException(SocketProtocolError.ProtocolError, "Nested batch messages are not supported."));
        return;
      }

      var socketEvent = new SocketEvent(item.Service, item.Operation, item.Payload ?? []);
      var authorization = await Options.Authorizer.AuthorizeEventAsync(this, socketEvent, cancellationToken);
      if (!authorization.Succeeded)
      {
        if (!Options.CloseOnUnauthorizedEvent)
          continue;

        await CloseAsync(new SocketProtocolException(SocketProtocolError.Unauthorized, authorization.Error));
        return;
      }

      try
      {
        await Handler.HandleEventAsync(this, socketEvent, cancellationToken);
      }
      catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
      {
        throw;
      }
      catch (Exception ex)
      {
        ObserveHandlerFailed(item.Service, item.Operation, ex);
      }
    }
  }

  async ValueTask<SocketBatchResult> HandleBatchEventItemAsync(
      SocketBatchItem item,
      CancellationToken cancellationToken)
  {
    try
    {
      if (IsInternalBatch(item.Service, item.Operation))
        return BatchFailed(item.Id, SocketProtocolError.ProtocolError, "Nested batch messages are not supported.");

      var socketEvent = new SocketEvent(item.Service, item.Operation, item.Payload ?? []);
      var authorization = await Options.Authorizer.AuthorizeEventAsync(this, socketEvent, cancellationToken);
      if (!authorization.Succeeded)
        return BatchFailed(item.Id, SocketProtocolError.Unauthorized, authorization.Error);

      await Handler.HandleEventAsync(this, socketEvent, cancellationToken);
      return new SocketBatchResult(item.Id, true, [], null, null);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      throw;
    }
    catch (Exception ex)
    {
      var failure = HandlerFailure(item.Service, item.Operation, ex);
      return BatchFailed(item.Id, failure.ErrorCode, failure.ErrorMessage);
    }
  }

  static SocketBatchResult BatchFailed(int id, string errorCode, string errorMessage)
  {
    return new SocketBatchResult(id, false, [], errorCode, errorMessage);
  }

  static SocketBatchItem[] ValidateBatch(IEnumerable<SocketBatchItem> items)
  {
    var batch = items?.ToArray() ?? [];
    if (batch.Length == 0)
      throw new ArgumentException("Batch must contain at least one item.", nameof(items));

    return batch;
  }

  static SocketFastBatchItem[] ValidateFastBatch(IEnumerable<SocketFastBatchItem> items)
  {
    var batch = items?.ToArray() ?? [];
    if (batch.Length == 0)
      throw new ArgumentException("Batch must contain at least one item.", nameof(items));

    return batch;
  }

  static bool IsBatchRequest(SocketRequest request)
  {
    return IsInternalBatch(request.Service, request.Operation)
        && (string.Equals(request.Operation, BatchRequestOperation, StringComparison.Ordinal)
            || string.Equals(request.Operation, BatchEventRequestOperation, StringComparison.Ordinal));
  }

  static bool IsBatchEvent(SocketEvent socketEvent)
  {
    return IsInternalBatch(socketEvent.Service, socketEvent.Operation)
        && string.Equals(socketEvent.Operation, BatchEventOperation, StringComparison.Ordinal);
  }

  static bool IsInternalBatch(string service, string operation)
  {
    return string.Equals(service, InternalService, StringComparison.Ordinal)
        && (string.Equals(operation, BatchRequestOperation, StringComparison.Ordinal)
            || string.Equals(operation, BatchEventOperation, StringComparison.Ordinal)
            || string.Equals(operation, BatchEventRequestOperation, StringComparison.Ordinal));
  }
}
