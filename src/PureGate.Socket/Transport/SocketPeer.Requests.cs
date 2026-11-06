using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  async ValueTask DispatchRequestAsync(SocketFrame frame)
  {
    SocketResponse response;
    var requestTimeout = RentRequestCancellation();
    if (frame.DeadlineMs > 0)
      requestTimeout.CancelAfter(TimeSpan.FromMilliseconds(frame.DeadlineMs));

    ActiveRequestCancellations[frame.MessageId] = requestTimeout;
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
      response = frame.DeadlineMs > 0
          ? SocketResponse.Failed(SocketProtocolError.Timeout, "Request deadline elapsed.")
          : SocketResponse.Failed(SocketProtocolError.Cancelled, "Request was cancelled.");
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      response = SocketResponse.Failed(SocketProtocolError.HandlerFailed, ex.Message);
    }

    response ??= SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Request was not handled.");
    try
    {
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
      ActiveRequestCancellations.TryRemove(frame.MessageId, out _);
      ReturnRequestCancellation(requestTimeout);
    }
  }

  async ValueTask DispatchFastRequestAsync(SocketFrame frame)
  {
    SocketResponse response;
    var requestTimeout = RentRequestCancellation();
    if (frame.DeadlineMs > 0)
      requestTimeout.CancelAfter(TimeSpan.FromMilliseconds(frame.DeadlineMs));

    ActiveRequestCancellations[frame.MessageId] = requestTimeout;
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
      response = frame.DeadlineMs > 0
          ? SocketResponse.Failed(SocketProtocolError.Timeout, "Request deadline elapsed.")
          : SocketResponse.Failed(SocketProtocolError.Cancelled, "Request was cancelled.");
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      response = SocketResponse.Failed(SocketProtocolError.HandlerFailed, ex.Message);
    }

    response ??= SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Fast request was not handled.");
    try
    {
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
      ActiveRequestCancellations.TryRemove(frame.MessageId, out _);
      ReturnRequestCancellation(requestTimeout);
    }
  }

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
    catch (OperationCanceledException)
    {
      throw;
    }
    catch (Exception ex)
    {
      results.WriteResult(
          item.Id,
          false,
          ReadOnlyMemory<byte>.Empty,
          SocketProtocolError.HandlerFailed,
          ex.Message);
    }
  }

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
        await CloseAsync(new SocketProtocolException(SocketProtocolError.Unauthorized, authorization.Error));
        return;
      }

      await Handler.HandleEventAsync(
          this,
          socketEvent,
          Lifetime.Token);
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
      await CloseAsync(ex);
    }
  }

  void DispatchResponse(SocketFrame frame)
  {
    if (!PendingRequests.TryGetValue(frame.MessageId, out var pending))
      return;

    pending.TrySetResult(new SocketRequestResult
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
    var candidate = new SocketIdempotencyEntry();
    var entry = Options.IdempotencyCache.GetOrAdd(key, candidate);
    if (!ReferenceEquals(entry, candidate))
      return await entry.Task.WaitAsync(cancellationToken);

    try
    {
      var response = await HandleRequestAsync(request, cancellationToken)
          ?? SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "Request was not handled.");
      candidate.TrySetResult(response);
      return response;
    }
    catch (OperationCanceledException ex)
    {
      Options.IdempotencyCache.Remove(key);
      candidate.TrySetException(ex);
      throw;
    }
    catch (Exception ex)
    {
      var response = SocketResponse.Failed(SocketProtocolError.HandlerFailed, ex.Message);
      candidate.TrySetResult(response);
      return response;
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
    catch (OperationCanceledException)
    {
      throw;
    }
    catch (Exception ex)
    {
      return BatchFailed(item.Id, SocketProtocolError.HandlerFailed, ex.Message);
    }
  }

  async ValueTask HandleBatchEventAsync(
      SocketEvent batchEvent,
      CancellationToken cancellationToken)
  {
    var items = SocketBatchCodec.DeserializeItems(batchEvent.Payload.Span);
    foreach (var item in items)
    {
      var result = await HandleBatchEventItemAsync(item, cancellationToken);
      if (!result.Succeeded)
        throw new SocketProtocolException(result.ErrorCode, result.ErrorMessage);
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
    catch (OperationCanceledException)
    {
      throw;
    }
    catch (Exception ex)
    {
      return BatchFailed(item.Id, SocketProtocolError.HandlerFailed, ex.Message);
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
