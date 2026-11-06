using System.Diagnostics;
using System.Net;
using PureGate.Socket.Framing;
using PureGate.Socket.Handling;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

const int DefaultIterations = 5_000;
const int RequestBatchSize = 64;
const int FastEchoOperation = 1;

var quick = args.Contains("--quick");
var iterations = GetIntArg(args, "--iterations", quick ? 500 : DefaultIterations);
var streamBytes = GetIntArg(args, "--stream-bytes", quick ? 4 * 1024 * 1024 : 32 * 1024 * 1024);
var concurrentClients = GetIntArg(args, "--clients", Environment.ProcessorCount);
var suite = GetStringArg(args, "--suite", "all");
var jsonPath = GetStringArg(args, "--json", null);

Console.WriteLine("PureGate.Socket benchmarks");
Console.WriteLine($"Runtime: {Environment.Version}");
Console.WriteLine($"Configuration: {(IsReleaseBuild() ? "Release" : "Debug")}");
Console.WriteLine($"Suite: {suite}{(quick ? " (quick)" : string.Empty)}");
Console.WriteLine($"Iterations: {iterations:N0}");
Console.WriteLine();

if (suite is "all" or "matrix")
{
  var matrix = await MatrixBenchmarks.RunAsync(quick, GetStringArg(args, "--filter", null));
  Console.WriteLine();
  if (jsonPath is not null)
    MatrixBenchmarks.WriteJson(jsonPath, matrix);
}

if (suite is not ("all" or "classic"))
  return;

Console.WriteLine("Whole scenario");
await RunAsync("frame roundtrip", iterations, () => FrameRoundtripAsync(iterations));
await RunAsync("rpc echo connect+steady", iterations, () => RpcEchoScenarioAsync(iterations));
await RunAsync("fast rpc echo connect+steady", iterations, () => FastRpcEchoScenarioAsync(iterations));
await RunAsync("request batch connect+steady", RoundUp(iterations, RequestBatchSize), () => RequestBatchScenarioAsync(iterations));
await RunAsync("fast request batch connect+steady", RoundUp(iterations, RequestBatchSize), () => FastRequestBatchScenarioAsync(iterations));
await RunAsync("pull stream connect+steady", streamBytes, () => PullStreamScenarioAsync(streamBytes), "bytes");
await RunAsync("concurrent rpc connect+steady", iterations * concurrentClients, () => ConcurrentRpcScenarioAsync(concurrentClients, iterations));

Console.WriteLine("Steady state");
await RunWithContextAsync(
    "rpc echo steady",
    iterations,
    StartEchoClientAsync,
    context => RpcEchoAsync(context.Client, iterations));
await RunWithContextAsync(
    "fast rpc echo steady",
    iterations,
    StartEchoClientAsync,
    context => FastRpcEchoAsync(context.Client, iterations));
await RunWithContextAsync(
    "request batch steady",
    RoundUp(iterations, RequestBatchSize),
    StartEchoClientAsync,
    context => RequestBatchAsync(context.Client, iterations));
await RunWithContextAsync(
    "fast request batch steady",
    RoundUp(iterations, RequestBatchSize),
    StartEchoClientAsync,
    context => FastRequestBatchAsync(context.Client, iterations));
await RunWithContextAsync(
    "pull stream steady",
    streamBytes,
    () => StartStreamClientAsync(streamBytes),
    context => PullStreamAsync(context.Client, streamBytes),
    "bytes");
await RunWithContextAsync(
    "concurrent rpc steady",
    iterations * concurrentClients,
    () => StartConcurrentEchoClientsAsync(concurrentClients),
    context => ConcurrentRpcAsync(context.Clients, iterations));
Console.WriteLine();

static async Task RunAsync(
    string name,
    long operations,
    Func<Task> action,
    string unit = "ops")
{
  GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
  GC.WaitForPendingFinalizers();
  GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);

  var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
  var gen0Before = GC.CollectionCount(0);
  var gen1Before = GC.CollectionCount(1);
  var gen2Before = GC.CollectionCount(2);
  var stopwatch = Stopwatch.StartNew();

  await action();

  stopwatch.Stop();
  var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
  var seconds = Math.Max(stopwatch.Elapsed.TotalSeconds, 0.000001);
  Console.WriteLine($"{name}:");
  Console.WriteLine($"  elapsed:       {stopwatch.Elapsed.TotalMilliseconds:N2} ms");
  Console.WriteLine($"  throughput:    {operations / seconds:N0} {unit}/s");
  Console.WriteLine($"  allocated:     {allocated / 1024.0 / 1024.0:N2} MiB");
  Console.WriteLine($"  alloc/{Singular(unit)}:     {(double)allocated / Math.Max(operations, 1):N2} bytes");
  Console.WriteLine($"  collections:   gen0={GC.CollectionCount(0) - gen0Before}, gen1={GC.CollectionCount(1) - gen1Before}, gen2={GC.CollectionCount(2) - gen2Before}");
  Console.WriteLine();
}

static async Task RunWithContextAsync<TContext>(
    string name,
    long operations,
    Func<ValueTask<TContext>> setup,
    Func<TContext, Task> action,
    string unit = "ops")
    where TContext : IAsyncDisposable
{
  await using var context = await setup();
  GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
  GC.WaitForPendingFinalizers();
  GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);

  var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
  var gen0Before = GC.CollectionCount(0);
  var gen1Before = GC.CollectionCount(1);
  var gen2Before = GC.CollectionCount(2);
  var stopwatch = Stopwatch.StartNew();

  await action(context);

  stopwatch.Stop();
  var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
  var seconds = Math.Max(stopwatch.Elapsed.TotalSeconds, 0.000001);
  Console.WriteLine($"{name}:");
  Console.WriteLine($"  elapsed:       {stopwatch.Elapsed.TotalMilliseconds:N2} ms");
  Console.WriteLine($"  throughput:    {operations / seconds:N0} {unit}/s");
  Console.WriteLine($"  allocated:     {allocated / 1024.0 / 1024.0:N2} MiB");
  Console.WriteLine($"  alloc/{Singular(unit)}:     {(double)allocated / Math.Max(operations, 1):N2} bytes");
  Console.WriteLine($"  collections:   gen0={GC.CollectionCount(0) - gen0Before}, gen1={GC.CollectionCount(1) - gen1Before}, gen2={GC.CollectionCount(2) - gen2Before}");
  Console.WriteLine();
}

static async Task FrameRoundtripAsync(int iterations)
{
  var payload = new byte[128];
  Random.Shared.NextBytes(payload);
  using var stream = new MemoryStream();
  var writer = new SocketFrameWriter(stream, 1024 * 1024);
  for (var i = 0; i < iterations; i++)
  {
    await writer.WriteAsync(new SocketFrame
    {
      Version = SocketFabricOptions.CurrentProtocolVersion,
      MessageId = i + 1,
      Kind = SocketMessageKind.Request,
      Service = "bench",
      Operation = "frame",
      Payload = payload
    }, CancellationToken.None);
  }

  stream.Position = 0;
  var reader = new SocketFrameReader(stream, 1024 * 1024);
  for (var i = 0; i < iterations; i++)
  {
    var frame = await reader.ReadAsync(CancellationToken.None);
    if (frame is null || frame.MessageId != i + 1 || frame.PayloadLength != payload.Length)
      throw new InvalidOperationException("Frame roundtrip returned invalid data.");
  }
}

static async Task RpcEchoScenarioAsync(int iterations)
{
  await using var context = await StartEchoClientAsync();
  await RpcEchoAsync(context.Client, iterations);
}

static async Task FastRpcEchoScenarioAsync(int iterations)
{
  await using var context = await StartEchoClientAsync();
  await FastRpcEchoAsync(context.Client, iterations);
}

static async Task RpcEchoAsync(SocketPeer client, int iterations)
{
  var payload = CreatePayload();
  for (var i = 0; i < iterations; i++)
  {
    var result = await client.RequestAsync("bench", "echo", payload, TimeSpan.FromSeconds(10));
    if (!result.Succeeded || !result.Payload.Span.SequenceEqual(payload))
      throw new InvalidOperationException("RPC echo returned invalid data.");
  }
}

static async Task FastRpcEchoAsync(SocketPeer client, int iterations)
{
  var payload = CreatePayload();
  for (var i = 0; i < iterations; i++)
  {
    var result = await client.RequestFastAsync(FastEchoOperation, payload, TimeSpan.FromSeconds(10));
    if (!result.Succeeded || !result.Payload.Span.SequenceEqual(payload))
      throw new InvalidOperationException("Fast RPC echo returned invalid data.");
  }
}

static async Task RequestBatchScenarioAsync(int iterations)
{
  await using var context = await StartEchoClientAsync();
  await RequestBatchAsync(context.Client, iterations);
}

static async Task FastRequestBatchScenarioAsync(int iterations)
{
  await using var context = await StartEchoClientAsync();
  await FastRequestBatchAsync(context.Client, iterations);
}

static async Task RequestBatchAsync(SocketPeer client, int iterations)
{
  var payload = CreatePayload();
  var items = Enumerable
      .Range(0, RequestBatchSize)
      .Select(index => new SocketBatchItem(index, "bench", "echo", payload))
      .ToArray();
  var batches = Math.Max(1, RoundUp(iterations, RequestBatchSize) / RequestBatchSize);

  for (var i = 0; i < batches; i++)
  {
    var results = await client.RequestBatchAsync(items, TimeSpan.FromSeconds(10));
    if (results.Count != RequestBatchSize || results.Any(result => !result.Succeeded || !result.Payload.AsSpan().SequenceEqual(payload)))
      throw new InvalidOperationException("Batch returned invalid data.");
  }
}

static async Task FastRequestBatchAsync(SocketPeer client, int iterations)
{
  var payload = CreatePayload();
  var items = Enumerable
      .Range(0, RequestBatchSize)
      .Select(index => new SocketFastBatchItem(index, FastEchoOperation, payload))
      .ToArray();
  var batches = Math.Max(1, RoundUp(iterations, RequestBatchSize) / RequestBatchSize);

  for (var i = 0; i < batches; i++)
  {
    var results = await client.RequestFastBatchAsync(items, TimeSpan.FromSeconds(10));
    if (results.Count != RequestBatchSize || results.Any(result => !result.Succeeded || !result.Payload.AsSpan().SequenceEqual(payload)))
      throw new InvalidOperationException("Fast batch returned invalid data.");
  }
}

static int RoundUp(int value, int multiple)
{
  return ((value + multiple - 1) / multiple) * multiple;
}

static string Singular(string unit)
{
  return unit == "ops" ? "op" : unit.TrimEnd('s');
}

static async Task PullStreamScenarioAsync(int streamBytes)
{
  await using var context = await StartStreamClientAsync(streamBytes);
  await PullStreamAsync(context.Client, streamBytes);
}

static async Task PullStreamAsync(SocketPeer client, int streamBytes)
{
  await using var socketStream = await client.OpenStreamAsync("bench", "stream", Array.Empty<byte>());
  var copied = await CopyAndCountAsync(socketStream);
  if (copied != streamBytes)
    throw new InvalidOperationException("Stream returned invalid length.");
}

static async Task ConcurrentRpcScenarioAsync(int clients, int iterationsPerClient)
{
  await using var context = await StartConcurrentEchoClientsAsync(clients);
  await ConcurrentRpcAsync(context.Clients, iterationsPerClient);
}

static async Task ConcurrentRpcAsync(SocketPeer[] clients, int iterationsPerClient)
{
  var payload = CreatePayload();
  await Task.WhenAll(clients.Select(client => Task.Run(async () =>
  {
    for (var i = 0; i < iterationsPerClient; i++)
    {
      var result = await client.RequestAsync("bench", "echo", payload, TimeSpan.FromSeconds(10));
      if (!result.Succeeded || !result.Payload.Span.SequenceEqual(payload))
        throw new InvalidOperationException("Concurrent RPC returned invalid data.");
    }
  })));
}

static async ValueTask<long> CopyAndCountAsync(Stream stream)
{
  var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(128 * 1024);
  try
  {
    long total = 0;
    while (true)
    {
      var read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length));
      if (read == 0)
        return total;

      total += read;
    }
  }
  finally
  {
    System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
  }
}

static byte[] CreatePayload()
{
  var payload = new byte[128];
  Random.Shared.NextBytes(payload);
  return payload;
}

static async ValueTask<BenchmarkPeer> StartEchoClientAsync()
{
  var listener = await StartEchoListenerAsync();
  var client = await ConnectClientAsync(listener.EndPoint);
  return new BenchmarkPeer(listener, client);
}

static async ValueTask<BenchmarkPeer> StartStreamClientAsync(int streamBytes)
{
  var listener = new SocketListener(BenchmarkOptions("server"));
  await listener.StartAsync(
      new IPEndPoint(IPAddress.Loopback, 0),
      new BenchmarkHandler
      {
        OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new RepeatingByteStream(streamBytes)))
      });
  var client = await ConnectClientAsync(listener.EndPoint);
  return new BenchmarkPeer(listener, client);
}

static async ValueTask<BenchmarkPeers> StartConcurrentEchoClientsAsync(int clients)
{
  var listener = await StartEchoListenerAsync();
  var socketClients = new SocketPeer[clients];
  try
  {
    for (var i = 0; i < socketClients.Length; i++)
      socketClients[i] = await ConnectClientAsync(listener.EndPoint);
    return new BenchmarkPeers(listener, socketClients);
  }
  catch
  {
    foreach (var client in socketClients)
    {
      if (client is not null)
        await client.DisposeAsync();
    }

    await listener.DisposeAsync();
    throw;
  }
}

static async ValueTask<SocketListener> StartEchoListenerAsync()
{
  var listener = new SocketListener(BenchmarkOptions("server"));
  await listener.StartAsync(
      new IPEndPoint(IPAddress.Loopback, 0),
      new BenchmarkHandler
      {
        OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse
        {
          Succeeded = true,
          Payload = request.Payload
        }),
        OnFastRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse
        {
          Succeeded = request.OperationId == FastEchoOperation,
          Payload = request.OperationId == FastEchoOperation ? request.Payload : ReadOnlyMemory<byte>.Empty,
          ErrorCode = request.OperationId == FastEchoOperation ? null : SocketProtocolError.UnsupportedOperation,
          ErrorMessage = request.OperationId == FastEchoOperation ? null : "Unknown fast operation."
        })
      });
  return listener;
}

static async ValueTask<SocketPeer> ConnectClientAsync(IPEndPoint endPoint)
{
  var connector = new SocketConnector(BenchmarkOptions("client"));
  return await connector.ConnectAsync(endPoint, new BenchmarkHandler());
}

static SocketFabricOptions BenchmarkOptions(string nodeId)
{
  return new SocketFabricOptions
  {
    NodeId = nodeId,
    Codec = RawSocketMessageCodec.Shared,
    MaxFrameSizeBytes = 1024 * 1024,
    MaxConcurrentHandlers = Environment.ProcessorCount * 2,
    MaxQueuedHandlers = 100_000,
    MaxInFlightRequests = 100_000,
    StreamChunkBytes = 128 * 1024,
    MaxStreamBufferChunks = 64,
    RequestTimeout = TimeSpan.FromSeconds(30)
  };
}

static int GetIntArg(string[] args, string name, int defaultValue)
{
  var index = Array.IndexOf(args, name);
  if (index < 0 || index + 1 >= args.Length)
    return defaultValue;

  return int.TryParse(args[index + 1], out var value) && value > 0
      ? value
      : defaultValue;
}

static string GetStringArg(string[] args, string name, string defaultValue)
{
  var index = Array.IndexOf(args, name);
  return index < 0 || index + 1 >= args.Length ? defaultValue : args[index + 1];
}

static bool IsReleaseBuild()
{
#if DEBUG
  return false;
#else
  return true;
#endif
}

sealed class BenchmarkHandler : ISocketPeerHandler
{
  public Func<SocketPeer, SocketRequest, CancellationToken, ValueTask<SocketResponse>> OnRequest { get; init; }
  public Func<SocketPeer, SocketFastRequest, CancellationToken, ValueTask<SocketResponse>> OnFastRequest { get; init; }
  public Func<SocketPeer, SocketStreamRequest, CancellationToken, ValueTask<SocketStreamResponse>> OnStream { get; init; }

  public ValueTask<SocketResponse> HandleRequestAsync(
      SocketPeer peer,
      SocketRequest request,
      CancellationToken cancellationToken)
  {
    return OnRequest?.Invoke(peer, request, cancellationToken)
        ?? ValueTask.FromResult(SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "No handler."));
  }

  public ValueTask<SocketResponse> HandleFastRequestAsync(
      SocketPeer peer,
      SocketFastRequest request,
      CancellationToken cancellationToken)
  {
    return OnFastRequest?.Invoke(peer, request, cancellationToken)
        ?? ValueTask.FromResult(SocketResponse.Failed(SocketProtocolError.UnsupportedOperation, "No fast handler."));
  }

  public ValueTask<SocketStreamResponse> HandleStreamAsync(
      SocketPeer peer,
      SocketStreamRequest request,
      CancellationToken cancellationToken)
  {
    return OnStream?.Invoke(peer, request, cancellationToken)
        ?? ValueTask.FromResult(SocketStreamResponse.Failed(SocketProtocolError.UnsupportedOperation, "No stream handler."));
  }
}

sealed class BenchmarkPeer(SocketListener listener, SocketPeer client) : IAsyncDisposable
{
  public SocketPeer Client { get; } = client;

  public async ValueTask DisposeAsync()
  {
    await Client.DisposeAsync();
    await listener.DisposeAsync();
  }
}

sealed class BenchmarkPeers(SocketListener listener, SocketPeer[] clients) : IAsyncDisposable
{
  public SocketPeer[] Clients { get; } = clients;

  public async ValueTask DisposeAsync()
  {
    foreach (var client in Clients)
    {
      if (client is not null)
        await client.DisposeAsync();
    }

    await listener.DisposeAsync();
  }
}

sealed class RepeatingByteStream(long length) : Stream
{
  long PositionValue;

  public override bool CanRead => true;
  public override bool CanSeek => false;
  public override bool CanWrite => false;
  public override long Length => length;
  public override long Position
  {
    get => PositionValue;
    set => throw new NotSupportedException();
  }

  public override int Read(byte[] buffer, int offset, int count)
  {
    return Read(buffer.AsSpan(offset, count));
  }

  public override int Read(Span<byte> buffer)
  {
    if (PositionValue >= length)
      return 0;

    var read = (int)Math.Min(buffer.Length, length - PositionValue);
    buffer.Slice(0, read).Fill(0x5A);
    PositionValue += read;
    return read;
  }

  public override ValueTask<int> ReadAsync(
      Memory<byte> buffer,
      CancellationToken cancellationToken = default)
  {
    cancellationToken.ThrowIfCancellationRequested();
    return ValueTask.FromResult(Read(buffer.Span));
  }

  public override void Flush()
  {
  }

  public override long Seek(long offset, SeekOrigin origin)
  {
    throw new NotSupportedException();
  }

  public override void SetLength(long value)
  {
    throw new NotSupportedException();
  }

  public override void Write(byte[] buffer, int offset, int count)
  {
    throw new NotSupportedException();
  }
}
