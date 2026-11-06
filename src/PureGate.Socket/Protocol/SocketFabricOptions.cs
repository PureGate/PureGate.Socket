using PureGate.Socket.Observability;
using PureGate.Socket.Security;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Protocol;

public sealed class SocketFabricOptions
{
  public const int CurrentProtocolVersion = 1;
  const int MaxWindowLimit = 512 * 1024 * 1024;

  public string NodeId { get; init; } = Environment.MachineName;
  public int ProtocolVersion { get; init; } = CurrentProtocolVersion;
  public int MaxFrameSizeBytes { get; init; } = 1024 * 1024;
  public int MaxDecompressedPayloadSizeBytes { get; init; } = 128 * 1024 * 1024;
  public int MaxChunkPayloadBytes { get; init; } = 256 * 1024;
  public int MaxChunkedMessages { get; init; } = 1024;
  public long MaxChunkedBytesInProgress { get; init; } = 256L * 1024 * 1024;
  public int MaxInFlightRequests { get; init; } = 1024;
  public int MaxConcurrentHandlers { get; init; } = 16;
  public int MaxQueuedHandlers { get; init; } = 4096;
  public int MaxIdempotencyEntries { get; init; } = 100_000;
  // Bytes a stream consumer lets its producer send ahead of what the application has read. The window
  // doubles automatically, up to MaxStreamWindowBytes, when the producer runs out of window while the
  // consumer is waiting for data (the window, not the network, is then the limit).
  public int StreamWindowBytes { get; init; } = 1024 * 1024;
  public int MaxStreamWindowBytes { get; init; } = 64 * 1024 * 1024;
  public int StreamChunkBytes { get; init; } = 64 * 1024;
  public int CompressionMinPayloadBytes { get; init; } = 16 * 1024;
  public SocketPayloadCompression PayloadCompression { get; init; } = SocketPayloadCompression.None;
  public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(10);
  public TimeSpan IdempotencyTtl { get; init; } = TimeSpan.FromMinutes(10);
  public TimeSpan ChunkTimeout { get; init; } = TimeSpan.FromSeconds(30);
  public TimeSpan HandshakeTimeout { get; init; } = TimeSpan.FromSeconds(5);

  // Longest gap allowed between two stream chunks once a stream has started. The open timeout only
  // covers the time until the stream starts.
  public TimeSpan StreamIdleTimeout { get; init; } = TimeSpan.FromSeconds(30);

  // When false (the default), handler exception messages are replaced with a generic message before
  // they are sent to the remote peer. The full exception always reaches ISocketObserver.HandlerFailed.
  public bool IncludeExceptionDetails { get; init; }

  // Whether an event rejected by the authorizer closes the connection. Events have no response, so
  // closing is the only signal the sender gets.
  public bool CloseOnUnauthorizedEvent { get; init; } = true;
  public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(15);
  public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(45);
  public ISocketMessageCodec Codec { get; init; } = JsonSocketMessageCodec.Shared;
  public ISocketAuthenticator Authenticator { get; init; } = NoSocketAuthenticator.Shared;
  public ISocketAuthorizer Authorizer { get; init; } = NoSocketAuthorizer.Shared;
  public ISocketStreamFilter StreamFilter { get; init; } = NoSocketStreamFilter.Shared;
  public ISocketObserver Observer { get; init; } = NoSocketObserver.Shared;
  internal SocketIdempotencyCache IdempotencyCache { get; init; } = new();

  internal SocketFabricOptions Normalize()
  {
    var heartbeatInterval = HeartbeatInterval <= TimeSpan.Zero ? TimeSpan.FromSeconds(15) : HeartbeatInterval;
    var minimumIdleTimeout = heartbeatInterval.Ticks > TimeSpan.MaxValue.Ticks / 3
        ? TimeSpan.MaxValue
        : TimeSpan.FromTicks(heartbeatInterval.Ticks * 3);
    var configuredIdleTimeout = IdleTimeout <= TimeSpan.Zero ? TimeSpan.FromSeconds(45) : IdleTimeout;
    var idleTimeout = configuredIdleTimeout < minimumIdleTimeout
        ? minimumIdleTimeout
        : configuredIdleTimeout;

    return new SocketFabricOptions
    {
      NodeId = string.IsNullOrWhiteSpace(NodeId) ? Environment.MachineName : NodeId.Trim(),
      ProtocolVersion = ProtocolVersion == SocketFabricOptions.CurrentProtocolVersion
          ? ProtocolVersion
          : SocketFabricOptions.CurrentProtocolVersion,
      MaxFrameSizeBytes = Math.Clamp(MaxFrameSizeBytes, 1024, 128 * 1024 * 1024),
      MaxDecompressedPayloadSizeBytes = Math.Clamp(MaxDecompressedPayloadSizeBytes, 1024, 1024 * 1024 * 1024),
      MaxChunkPayloadBytes = Math.Clamp(MaxChunkPayloadBytes, 1, Math.Max(1, MaxFrameSizeBytes / 4)),
      MaxChunkedMessages = Math.Clamp(MaxChunkedMessages, 1, 100_000),
      MaxChunkedBytesInProgress = Math.Clamp(MaxChunkedBytesInProgress, 1024, 16L * 1024 * 1024 * 1024),
      MaxInFlightRequests = Math.Clamp(MaxInFlightRequests, 1, 100_000),
      MaxConcurrentHandlers = Math.Clamp(MaxConcurrentHandlers, 1, 1024),
      MaxQueuedHandlers = Math.Clamp(MaxQueuedHandlers, 1, 100_000),
      MaxIdempotencyEntries = Math.Clamp(MaxIdempotencyEntries, 1, 1_000_000),
      StreamWindowBytes = Math.Clamp(StreamWindowBytes, 64 * 1024, MaxWindowLimit),
      MaxStreamWindowBytes = Math.Clamp(MaxStreamWindowBytes, Math.Clamp(StreamWindowBytes, 64 * 1024, MaxWindowLimit), MaxWindowLimit),
      StreamChunkBytes = Math.Clamp(StreamChunkBytes, 1, Math.Max(1, MaxFrameSizeBytes / 4)),
      CompressionMinPayloadBytes = Math.Clamp(CompressionMinPayloadBytes, 0, 128 * 1024 * 1024),
      PayloadCompression = Enum.IsDefined(PayloadCompression) ? PayloadCompression : SocketPayloadCompression.None,
      RequestTimeout = RequestTimeout <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : RequestTimeout,
      IdempotencyTtl = IdempotencyTtl <= TimeSpan.Zero ? TimeSpan.FromMinutes(10) : IdempotencyTtl,
      ChunkTimeout = ChunkTimeout <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : ChunkTimeout,
      HandshakeTimeout = HandshakeTimeout <= TimeSpan.Zero ? TimeSpan.FromSeconds(5) : HandshakeTimeout,
      StreamIdleTimeout = StreamIdleTimeout <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : StreamIdleTimeout,
      IncludeExceptionDetails = IncludeExceptionDetails,
      CloseOnUnauthorizedEvent = CloseOnUnauthorizedEvent,
      HeartbeatInterval = heartbeatInterval,
      IdleTimeout = idleTimeout,
      Codec = Codec ?? JsonSocketMessageCodec.Shared,
      Authenticator = Authenticator ?? NoSocketAuthenticator.Shared,
      Authorizer = Authorizer ?? NoSocketAuthorizer.Shared,
      StreamFilter = StreamFilter ?? NoSocketStreamFilter.Shared,
      Observer = Observer ?? NoSocketObserver.Shared,
      IdempotencyCache = IdempotencyCache ?? new SocketIdempotencyCache()
    };
  }
}
