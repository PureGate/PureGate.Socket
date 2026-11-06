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
  const int InternalFastBatchOperationId = int.MinValue;

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
  readonly ConcurrentDictionary<long, CancellationTokenSource> ActiveRequestCancellations = new();
  readonly ConcurrentDictionary<ChunkKey, IncomingChunkedFrame> IncomingChunks = new();
  readonly ConcurrentQueue<PendingRequest> PendingRequestPool = new();
  readonly ConcurrentQueue<CancellationTokenSource> RequestCancellationPool = new();
  readonly SemaphoreSlim InFlightRequests;
  readonly Channel<SocketFrame> HandlerQueue;
  readonly TaskCompletionSource Closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
  readonly Task ReadTask;
  readonly Task HeartbeatTask;
  readonly Task ConnectedTask;
  readonly Task[] HandlerTasks;

  long NextMessageId;
  long LastReceivedTicks = DateTimeOffset.UtcNow.UtcTicks;
  int ClosedFlag;

  internal SocketPeer(
      TcpClient client,
      Stream stream,
      string localNodeId,
      string remoteNodeId,
      SocketFabricOptions options,
      ISocketPeerHandler handler)
  {
    Client = client;
    Stream = stream;
    LocalNodeId = localNodeId ?? string.Empty;
    RemoteNodeId = remoteNodeId ?? string.Empty;
    Options = options.Normalize();
    Handler = handler ?? EmptySocketPeerHandler.Shared;
    Reader = new SocketFrameReader(
        stream,
        Options.MaxFrameSizeBytes,
        Options.PayloadCompression,
        Options.MaxDecompressedPayloadSizeBytes);
    Writer = new SocketFrameWriter(
        stream,
        Options.MaxFrameSizeBytes,
        Options.PayloadCompression,
        Options.CompressionMinPayloadBytes);
    InFlightRequests = new SemaphoreSlim(Options.MaxInFlightRequests, Options.MaxInFlightRequests);
    HandlerQueue = Channel.CreateBounded<SocketFrame>(new BoundedChannelOptions(Options.MaxQueuedHandlers)
    {
      FullMode = BoundedChannelFullMode.Wait,
      SingleReader = false,
      SingleWriter = true
    });
    State = SocketPeerState.Connected;
    ConnectedTask = Task.Run(NotifyConnectedAsync);
    ReadTask = Task.Run(ReadLoopAsync);
    HeartbeatTask = Task.Run(HeartbeatLoopAsync);
    HandlerTasks = Enumerable
        .Range(0, Options.MaxConcurrentHandlers)
        .Select(_ => Task.Run(HandlerLoopAsync))
        .ToArray();
  }

  public string LocalNodeId { get; }
  public string RemoteNodeId { get; }
  public string ConnectionId { get; } = Guid.NewGuid().ToString("N");
  public SocketPeerState State { get; private set; } = SocketPeerState.Connecting;
  public Task Completion => Closed.Task;
  public ISocketMessageCodec Codec => Options.Codec;
}
