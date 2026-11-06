using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Handling;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Fabric;

public sealed class SocketClient : IAsyncDisposable
{
  readonly SocketFabricOptions Options;
  readonly SocketClientOptions ClientOptions;
  readonly SocketConnector Connector;
  readonly EndPoint EndPoint;
  readonly ISocketPeerHandler Handler;
  readonly SemaphoreSlim ConnectLock = new(1, 1);
  readonly CancellationTokenSource Lifetime = new();
  readonly Task KeepConnectedTask;
  SocketPeer Peer;
  int Disposed;

  // Connects to host:port, resolving the name again on every reconnect.
  public SocketClient(
      string host,
      int port,
      ISocketPeerHandler handler,
      SocketFabricOptions options = null,
      SocketClientOptions clientOptions = null)
      : this(new DnsEndPoint(host ?? throw new ArgumentNullException(nameof(host)), port), handler, options, clientOptions)
  {
  }

  // endPoint is an IPEndPoint or a DnsEndPoint.
  public SocketClient(
      EndPoint endPoint,
      ISocketPeerHandler handler,
      SocketFabricOptions options = null,
      SocketClientOptions clientOptions = null)
  {
    EndPoint = endPoint ?? throw new ArgumentNullException(nameof(endPoint));
    Handler = handler ?? EmptySocketPeerHandler.Shared;
    Options = (options ?? new SocketFabricOptions()).Normalize();
    ClientOptions = (clientOptions ?? SocketClientOptions.Default).Normalize();
    Connector = new SocketConnector(Options);
    if (ClientOptions.KeepConnected)
      KeepConnectedTask = Task.Run(KeepConnectedLoopAsync);
  }

  public async ValueTask<SocketPeer> GetPeerAsync(CancellationToken cancellationToken = default)
  {
    ThrowIfDisposed();
    var peer = Peer;
    if (peer?.State == SocketPeerState.Connected)
      return peer;

    using var connectCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Lifetime.Token);
    await ConnectLock.WaitAsync(connectCancellation.Token);
    try
    {
      peer = Peer;
      if (peer?.State == SocketPeerState.Connected)
        return peer;

      if (peer is not null)
        await peer.DisposeAsync();

      ObserveClientConnectStarted();
      try
      {
        Peer = await Connector.ConnectAsync(EndPoint, Handler, connectCancellation.Token);
      }
      catch (Exception ex)
      {
        if (ex is not OperationCanceledException || !connectCancellation.IsCancellationRequested)
          ObserveClientConnectFailed(ex);
        throw;
      }

      return Peer;
    }
    finally
    {
      ConnectLock.Release();
    }
  }

  public async ValueTask<SocketRequestResult> RequestAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await RequestAsync(service, operation, payload, SocketClientRequestOptions.Default, cancellationToken);
  }

  public ValueTask<SocketRequestResult> RequestAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      SocketClientRequestOptions options,
      CancellationToken cancellationToken = default)
  {
    return SendAsync(service, operation, payload, options, pooledResponse: false, cancellationToken);
  }

  async ValueTask<SocketRequestResult> SendAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      SocketClientRequestOptions options,
      bool pooledResponse,
      CancellationToken cancellationToken)
  {
    var retry = (options ?? SocketClientRequestOptions.Default).Normalize();
    var idempotencyKey = retry.Idempotent
        ? string.IsNullOrEmpty(retry.IdempotencyKey) ? Guid.NewGuid().ToString("N") : retry.IdempotencyKey
        : string.Empty;
    // Serialized once into a pooled buffer, reused by every attempt and returned at the end.
    var serialized = SocketPayloadWriter.Serialize(Options.Codec, payload);
    try
    {
      return await RequestWithRetriesAsync(service, operation, serialized, retry, idempotencyKey, pooledResponse, cancellationToken);
    }
    finally
    {
      serialized.Dispose();
    }
  }

  async ValueTask<SocketRequestResult> RequestWithRetriesAsync(
      string service,
      string operation,
      SocketPayload serialized,
      SocketClientRequestOptions retry,
      string idempotencyKey,
      bool pooledResponse,
      CancellationToken cancellationToken)
  {
    for (var attempt = 1; ; attempt++)
    {
      // The connection is shared by every caller of this client, so a retry never closes it. A timeout
      // retries on the same connection; a dead connection is replaced by GetPeerAsync on the next attempt.
      SocketPeer peer = null;
      try
      {
        peer = await GetPeerAsync(cancellationToken);
        var sentTicks = Environment.TickCount64;
        var result = await peer.RequestPayloadAsync(
            service,
            operation,
            serialized,
            TimeSpan.Zero,
            idempotencyKey,
            cancellationToken,
            pooledResponse);
        if (!ShouldRetry(result, retry, attempt))
          return result;

        // A result that is retried is dropped, so its pooled payload (if any) goes back now.
        result.ReleasePooled();

        // Before retrying a timeout, find out whether the connection is still alive. A half-open one is
        // closed by the check, so the retry goes out on a fresh connection instead of timing out again.
        if (result.ErrorCode == SocketProtocolError.Timeout)
          await peer.CheckLivenessAsync(sentTicks);
      }
      catch (SocketProtocolException ex) when (ShouldRetry(ex, retry, attempt))
      {
      }
      catch (IOException) when (ShouldRetry(retry, attempt))
      {
        await InvalidatePeerAsync(peer);
      }
      catch (SocketException) when (ShouldRetry(retry, attempt))
      {
        await InvalidatePeerAsync(peer);
      }

      if (retry.RetryDelay > TimeSpan.Zero)
        await Task.Delay(retry.RetryDelay, cancellationToken);
    }
  }

  // Drops a peer whose transport failed, but only if it is still the current one: another caller may
  // already have replaced it.
  async ValueTask InvalidatePeerAsync(SocketPeer peer)
  {
    if (peer is null || Volatile.Read(ref Disposed) != 0)
      return;

    try
    {
      await ConnectLock.WaitAsync();
    }
    catch (ObjectDisposedException)
    {
      // The client was disposed concurrently; it closes its peer itself.
      return;
    }

    try
    {
      if (!ReferenceEquals(Peer, peer))
        return;
      Peer = null;
    }
    finally
    {
      ConnectLock.Release();
    }

    await peer.DisposeAsync();
  }

  public async ValueTask<TResponse> RequestAsync<TRequest, TResponse>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await RequestAsync<TRequest, TResponse>(
        service,
        operation,
        payload,
        SocketClientRequestOptions.Default,
        cancellationToken);
  }

  public async ValueTask<TResponse> RequestAsync<TRequest, TResponse>(
      string service,
      string operation,
      TRequest payload,
      SocketClientRequestOptions options,
      CancellationToken cancellationToken = default)
  {
    // Deserialized here, so the response bytes can be pooled and returned right after.
    var result = await SendAsync(service, operation, payload, options, pooledResponse: true, cancellationToken);
    return SocketPeer.ReadTypedResult<TResponse>(result, Options.Codec);
  }

  public async ValueTask SendEventAsync<TPayload>(
      string service,
      string operation,
      TPayload payload,
      CancellationToken cancellationToken = default)
  {
    var peer = await GetPeerAsync(cancellationToken);
    await peer.SendEventAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<Stream> OpenStreamAsync<TRequest>(
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    var peer = await GetPeerAsync(cancellationToken);
    return await peer.OpenStreamAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<Stream> OpenStreamAsync(
      string service,
      string operation,
      byte[] payload,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var peer = await GetPeerAsync(cancellationToken);
    return await peer.OpenStreamAsync(service, operation, payload, timeout, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> RequestBatchAsync(
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var peer = await GetPeerAsync(cancellationToken);
    return await peer.RequestBatchAsync(items, timeout, cancellationToken);
  }

  public async ValueTask SendEventBatchAsync(
      IEnumerable<SocketBatchItem> items,
      CancellationToken cancellationToken = default)
  {
    var peer = await GetPeerAsync(cancellationToken);
    await peer.SendEventBatchAsync(items, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> SendEventBatchWithResultsAsync(
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    var peer = await GetPeerAsync(cancellationToken);
    return await peer.SendEventBatchWithResultsAsync(items, timeout, cancellationToken);
  }

  public async ValueTask CloseAsync()
  {
    await ConnectLock.WaitAsync();
    try
    {
      var peer = Interlocked.Exchange(ref Peer, null);
      if (peer is not null)
        await peer.DisposeAsync();
    }
    finally
    {
      ConnectLock.Release();
    }
  }

  public async ValueTask DisposeAsync()
  {
    if (Interlocked.Exchange(ref Disposed, 1) != 0)
      return;

    await Lifetime.CancelAsync();
    if (KeepConnectedTask is not null)
    {
      try
      {
        await KeepConnectedTask;
      }
      catch
      {
      }
    }

    await CloseAsync();
    Lifetime.Dispose();
    ConnectLock.Dispose();
  }

  async Task KeepConnectedLoopAsync()
  {
    var reconnectDelay = TimeSpan.Zero;
    while (!Lifetime.IsCancellationRequested)
    {
      try
      {
        if (reconnectDelay > TimeSpan.Zero)
        {
          var delay = ApplyJitter(reconnectDelay);
          ObserveClientReconnectScheduled(delay);
          await Task.Delay(delay, Lifetime.Token);
        }

        var peer = await GetPeerAsync(Lifetime.Token);
        reconnectDelay = ClientOptions.ReconnectInitialDelay;
        await peer.Completion.WaitAsync(Lifetime.Token);
      }
      catch (OperationCanceledException) when (Lifetime.IsCancellationRequested)
      {
        break;
      }
      catch (Exception ex)
      {
        if (!IsExpectedKeepConnectedFailure(ex))
          ObserveClientKeepConnectedLoopFailed(ex);

        reconnectDelay = reconnectDelay <= TimeSpan.Zero
            ? ClientOptions.ReconnectInitialDelay
            : IncreaseDelay(reconnectDelay);
      }
    }
  }

  TimeSpan ApplyJitter(TimeSpan delay)
  {
    if (delay <= TimeSpan.Zero || ClientOptions.ReconnectJitterRatio <= 0)
      return delay;

    var ratio = (Random.Shared.NextDouble() * 2 - 1) * ClientOptions.ReconnectJitterRatio;
    var ticks = delay.Ticks + (long)Math.Round(delay.Ticks * ratio);
    if (ticks < 0)
      ticks = 0;
    if (ticks > ClientOptions.ReconnectMaxDelay.Ticks)
      ticks = ClientOptions.ReconnectMaxDelay.Ticks;
    return TimeSpan.FromTicks(ticks);
  }

  TimeSpan IncreaseDelay(TimeSpan delay)
  {
    if (delay >= ClientOptions.ReconnectMaxDelay)
      return ClientOptions.ReconnectMaxDelay;
    if (delay.Ticks > ClientOptions.ReconnectMaxDelay.Ticks / 2)
      return ClientOptions.ReconnectMaxDelay;

    var next = TimeSpan.FromTicks(delay.Ticks * 2);
    return next > ClientOptions.ReconnectMaxDelay ? ClientOptions.ReconnectMaxDelay : next;
  }

  void ThrowIfDisposed()
  {
    if (Volatile.Read(ref Disposed) != 0)
      throw new ObjectDisposedException(nameof(SocketClient));
  }

  void ObserveClientConnectStarted()
  {
    try
    {
      Options.Observer.ClientConnectStarted(EndPoint);
    }
    catch
    {
    }
  }

  void ObserveClientConnectFailed(Exception exception)
  {
    try
    {
      Options.Observer.ClientConnectFailed(EndPoint, exception);
    }
    catch
    {
    }
  }

  void ObserveClientReconnectScheduled(TimeSpan delay)
  {
    try
    {
      Options.Observer.ClientReconnectScheduled(EndPoint, delay);
    }
    catch
    {
    }
  }

  void ObserveClientKeepConnectedLoopFailed(Exception exception)
  {
    try
    {
      Options.Observer.ClientKeepConnectedLoopFailed(EndPoint, exception);
    }
    catch
    {
    }
  }

  static bool IsExpectedKeepConnectedFailure(Exception exception)
  {
    return exception is SocketException
        or IOException
        or SocketProtocolException
        or OperationCanceledException;
  }

  static bool ShouldRetry(SocketRequestResult result, SocketClientRequestOptions retry, int attempt)
  {
    return IsRetryable(result?.ErrorCode) && ShouldRetry(retry, attempt);
  }

  static bool ShouldRetry(SocketProtocolException exception, SocketClientRequestOptions retry, int attempt)
  {
    return IsRetryable(exception.ErrorCode) && ShouldRetry(retry, attempt);
  }

  static bool IsRetryable(string errorCode)
  {
    return errorCode is SocketProtocolError.ConnectionClosed
        or SocketProtocolError.Timeout
        or SocketProtocolError.Overloaded;
  }

  static bool ShouldRetry(SocketClientRequestOptions retry, int attempt)
  {
    return retry.Idempotent && attempt < retry.MaxAttempts;
  }
}
