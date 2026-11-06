using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;
using PureGate.Socket.Framing;
using PureGate.Socket.Handling;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer : IAsyncDisposable
{
  const string InternalService = "socket";
  const string BatchRequestOperation = "batch.request";
  const string BatchEventOperation = "batch.event";
  const string BatchEventRequestOperation = "batch.event.request";

  readonly TcpClient Client;
  readonly Stream Stream;
  readonly SocketFabricOptions Options;
  readonly ISocketPeerHandler Handler;
  readonly SocketFrameReader Reader;
  readonly SocketFrameWriter Writer;
  readonly SemaphoreSlim WriteLock = new(1, 1);
  readonly CancellationTokenSource Lifetime = new();
  readonly ConcurrentDictionary<long, PendingRequest> PendingRequests = new();
  readonly ConcurrentDictionary<long, PendingStream> PendingStreams = new();
  readonly ConcurrentDictionary<long, ActiveRequest> ActiveRequests = new();
  readonly ConcurrentDictionary<ChunkKey, IncomingChunkedFrame> IncomingChunks = new();
  readonly ConcurrentQueue<PendingRequest> PendingRequestPool = new();
  readonly ConcurrentQueue<CancellationTokenSource> RequestCancellationPool = new();
  readonly TaskCompletionSource Closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
  readonly Task ReadTask;
  readonly Task HeartbeatTask;
  readonly Task ConnectedTask;

  long NextMessageId;
  // Environment.TickCount64 (milliseconds) of the last frame received; cheaper to read than the wall clock.
  long LastReceivedTicks = Environment.TickCount64;
  int InFlightCount;
  int ClosedFlag;

  // reader must be the reader that ran the handshake. A frame reader buffers ahead, so it may already hold
  // frames the remote sent right after the handshake; a fresh reader would silently lose them.
  // remoteMessageWindow is the number of handler-bound messages the remote said it can queue.
  internal SocketPeer(
      TcpClient client,
      Stream stream,
      SocketFrameReader reader,
      string localNodeId,
      string remoteNodeId,
      int remoteMessageWindow,
      SocketFabricOptions options,
      ISocketPeerHandler handler)
  {
    Client = client;
    Stream = stream;
    LocalNodeId = localNodeId ?? string.Empty;
    RemoteNodeId = remoteNodeId ?? string.Empty;
    MessageCredits = new MessageCredits(remoteMessageWindow);
    Options = options.Normalize();
    Handler = handler ?? EmptySocketPeerHandler.Shared;
    Reader = reader ?? throw new ArgumentNullException(nameof(reader));
    Reader.MaxFrameSizeBytes = Options.MaxFrameSizeBytes;
    Writer = new SocketFrameWriter(
        stream,
        Options.MaxFrameSizeBytes,
        Options.PayloadCompression,
        Options.CompressionMinPayloadBytes,
        useMethodTable: true);
    State = SocketPeerState.Connected;
    ConnectedTask = Task.Run(NotifyConnectedAsync);
    ReadTask = Task.Run(ReadLoopAsync);
    HeartbeatTask = Task.Run(HeartbeatLoopAsync);
  }

  // The in-flight limit is only ever checked without waiting, so a counter replaces the semaphore (which
  // took a lock on every request).
  bool TryAcquireInFlight()
  {
    while (true)
    {
      var current = Volatile.Read(ref InFlightCount);
      if (current >= Options.MaxInFlightRequests)
        return false;
      if (Interlocked.CompareExchange(ref InFlightCount, current + 1, current) == current)
        return true;
    }
  }

  void ReleaseInFlight()
  {
    Interlocked.Decrement(ref InFlightCount);
  }

  public string LocalNodeId { get; }
  public string RemoteNodeId { get; }
  public string ConnectionId { get; } = Guid.NewGuid().ToString("N");
  public SocketPeerState State { get; private set; } = SocketPeerState.Connecting;
  public Task Completion => Closed.Task;
  public ISocketMessageCodec Codec => Options.Codec;

  internal static SocketFrameReader CreateReader(Stream stream, SocketFabricOptions options)
  {
    // Created for the handshake, so it starts with the pre-authentication frame limit.
    return new SocketFrameReader(
        stream,
        options.MaxHandshakeFrameBytes,
        options.PayloadCompression,
        options.MaxDecompressedPayloadSizeBytes);
  }
}
