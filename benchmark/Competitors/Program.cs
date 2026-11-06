using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Bench.Grpc;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;
using StreamJsonRpc;

// Echoes a payload through each library, in two scenarios: calls one at a time (latency), and many
// callers sharing one connection (throughput). Client and server run in this process over loopback, so
// allocations count both sides.
//
// Each scenario runs for the same time (--duration), so every library gets the same wall-clock time to
// tier its code up, however fast it is. --warmup runs unmeasured calls first; with the default of 0, the
// first calls include JIT compilation, the same for every library.
//
//   --duration <seconds>  measured time per scenario (default 1)
//   --warmup <seconds>    unmeasured time before each scenario (default 0)
//   --callers <n>         concurrent callers sharing the connection (default 64)
//   --payload <bytes>     payload size (default 1024; gRPC allows up to 4 MB)

// Numbers print as 46,108 on every machine (some cultures would print 46.108, which reads as a decimal).
CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

var duration = TimeSpan.FromSeconds(Option("--duration", 1.0));
var warmup = TimeSpan.FromSeconds(Option("--warmup", 0.0));
var callers = (int)Option("--callers", 64);
var payloadBytes = (int)Option("--payload", 1024);

var payload = new byte[payloadBytes];
Random.Shared.NextBytes(payload);
var results = new List<Result>();

Console.WriteLine($".NET {Environment.Version}, {Environment.ProcessorCount} cores, tiered compilation {(Environment.GetEnvironmentVariable("DOTNET_TieredCompilation") == "0" ? "off" : "on")}");
Console.WriteLine($"Payload {payloadBytes:N0} B; each scenario runs {duration.TotalSeconds:0.##} s after {warmup.TotalSeconds:0.##} s of warmup; concurrent: {callers} callers on one connection");
Console.WriteLine();

await RunAsync("Raw socket (ceiling)", RawSocket.StartAsync, concurrent: false);
await RunAsync("PureGate.Socket", PureGateSocket.StartAsync, concurrent: true);
await RunAsync("gRPC (HTTP/2)", GrpcBench.StartAsync, concurrent: true);
await RunAsync("SignalR (MessagePack)", SignalRBench.StartAsync, concurrent: true);
await RunAsync("StreamJsonRpc (MessagePack)", StreamJsonRpcBench.StartAsync, concurrent: true);

Console.WriteLine();
var rows = new List<string[]>
{
  new[] { "Library", "Sequential calls/s", "p50", "p99", $"{callers} callers calls/s", "Alloc per call" }
};
foreach (var name in results.Select(r => r.Library).Distinct())
{
  var sequential = results.Single(r => r.Library == name && r.Scenario == "sequential");
  var concurrent = results.SingleOrDefault(r => r.Library == name && r.Scenario == "concurrent");
  rows.Add(
  [
    name,
    sequential.CallsPerSecond.ToString("N0"),
    $"{sequential.P50Us:N0} µs",
    $"{sequential.P99Us:N0} µs",
    concurrent is null ? "n/a" : concurrent.CallsPerSecond.ToString("N0"),
    $"{(concurrent ?? sequential).AllocPerCall:N0} B"
  ]);
}

PrintTable(rows);

// An aligned Markdown table: readable in the console and pasteable into docs. The first column is
// left-aligned, the numbers right-aligned.
static void PrintTable(List<string[]> rows)
{
  var widths = Enumerable.Range(0, rows[0].Length).Select(c => rows.Max(row => row[c].Length)).ToArray();
  string Line(string[] cells) =>
      "| " + string.Join(" | ", cells.Select((cell, c) => c == 0 ? cell.PadRight(widths[c]) : cell.PadLeft(widths[c]))) + " |";

  Console.WriteLine(Line(rows[0]));
  Console.WriteLine("|" + string.Join("|", widths.Select((width, c) => c == 0 ? new string('-', width + 2) : new string('-', width + 1) + ":")) + "|");
  foreach (var row in rows.Skip(1))
    Console.WriteLine(Line(row));
}

double Option(string name, double fallback)
{
  var index = Array.IndexOf(args, name);
  if (index < 0)
    return fallback;
  if (index + 1 >= args.Length || !double.TryParse(args[index + 1], NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || value < 0)
    throw new ArgumentException($"{name} needs a non-negative number.");
  return value;
}

async Task RunAsync(string library, Func<byte[], Task<IEchoClient>> start, bool concurrent)
{
  var setup = Stopwatch.StartNew();
  await using var client = await start(payload);
  Console.WriteLine($"{library}: connected in {setup.ElapsedMilliseconds} ms");

  // Sequential: one call at a time, measuring each.
  if (warmup > TimeSpan.Zero)
    await SequentialAsync(client, library, warmup, latencies: null);
  // Room for every latency up to 200k calls a second, allocated before measuring so it is not counted.
  var latencies = new double[(int)Math.Clamp(duration.TotalSeconds * 200_000, 1_000, 50_000_000)];
  var allocated = GC.GetTotalAllocatedBytes(precise: true);
  var watch = Stopwatch.StartNew();
  var calls = await SequentialAsync(client, library, duration, latencies);
  watch.Stop();
  var sequentialAlloc = (GC.GetTotalAllocatedBytes(precise: true) - allocated) / (double)calls;
  var recorded = Math.Min(calls, latencies.Length);
  Array.Sort(latencies, 0, recorded);
  var result = new Result(library, "sequential", calls / watch.Elapsed.TotalSeconds, latencies[recorded / 2], latencies[(int)(recorded * 0.99)], sequentialAlloc);
  results.Add(result);
  Console.WriteLine($"  sequential: {result.CallsPerSecond:N0} calls/s, p50 {result.P50Us:N0} µs, p99 {result.P99Us:N0} µs, {result.AllocPerCall:N0} B/call");

  if (!concurrent)
    return;

  // Concurrent: many callers sharing the one connection.
  if (warmup > TimeSpan.Zero)
    await ConcurrentAsync(client, library, warmup);
  allocated = GC.GetTotalAllocatedBytes(precise: true);
  watch.Restart();
  calls = await ConcurrentAsync(client, library, duration);
  watch.Stop();
  var concurrentResult = new Result(library, "concurrent", calls / watch.Elapsed.TotalSeconds, 0, 0, (GC.GetTotalAllocatedBytes(precise: true) - allocated) / (double)calls);
  results.Add(concurrentResult);
  Console.WriteLine($"  {callers} callers: {concurrentResult.CallsPerSecond:N0} calls/s, {concurrentResult.AllocPerCall:N0} B/call");
}

// Calls one at a time until the time is up; records each call's latency in microseconds when given room.
async Task<int> SequentialAsync(IEchoClient client, string library, TimeSpan time, double[] latencies)
{
  var calls = 0;
  var until = Stopwatch.GetTimestamp() + (long)(time.TotalSeconds * Stopwatch.Frequency);
  while (Stopwatch.GetTimestamp() < until)
  {
    var started = Stopwatch.GetTimestamp();
    var echoed = await client.EchoAsync(payload);
    if (latencies is not null && calls < latencies.Length)
      latencies[calls] = Stopwatch.GetElapsedTime(started).TotalMicroseconds;
    if (echoed != payload.Length)
      throw new InvalidOperationException($"{library} echoed {echoed} bytes");
    calls++;
  }

  return calls;
}

// Every caller calls in a loop until the time is up; returns the calls completed.
async Task<int> ConcurrentAsync(IEchoClient client, string library, TimeSpan time)
{
  var until = Stopwatch.GetTimestamp() + (long)(time.TotalSeconds * Stopwatch.Frequency);
  var counts = await Task.WhenAll(Enumerable.Range(0, callers).Select(_ => Task.Run(async () =>
  {
    var calls = 0;
    while (Stopwatch.GetTimestamp() < until)
    {
      if (await client.EchoAsync(payload) != payload.Length)
        throw new InvalidOperationException($"{library} echoed the wrong length");
      calls++;
    }

    return calls;
  })));
  return counts.Sum();
}

record Result(string Library, string Scenario, double CallsPerSecond, double P50Us, double P99Us, double AllocPerCall);

interface IEchoClient : IAsyncDisposable
{
  // Returns the length of the echoed payload.
  ValueTask<int> EchoAsync(byte[] payload);
}

// Length-prefixed echo over a bare socket: the floor for any protocol on this machine.
static class RawSocket
{
  public static async Task<IEchoClient> StartAsync(byte[] payload)
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var serverTask = Task.Run(async () =>
    {
      using var server = await listener.AcceptTcpClientAsync();
      server.NoDelay = true;
      var stream = server.GetStream();
      var buffer = new byte[4 + payload.Length];
      while (true)
      {
        try
        {
          await stream.ReadExactlyAsync(buffer.AsMemory(0, 4));
          var length = BinaryPrimitives.ReadInt32BigEndian(buffer);
          await stream.ReadExactlyAsync(buffer.AsMemory(4, length));
          await stream.WriteAsync(buffer.AsMemory(0, 4 + length));
        }
        catch
        {
          return;
        }
      }
    });

    var client = new TcpClient { NoDelay = true };
    await client.ConnectAsync((IPEndPoint)listener.LocalEndpoint);
    return new Client(client, listener, serverTask, payload.Length);
  }

  sealed class Client(TcpClient client, TcpListener listener, Task serverTask, int payloadBytes) : IEchoClient
  {
    readonly NetworkStream Stream = client.GetStream();
    readonly byte[] Buffer = new byte[4 + payloadBytes];

    public async ValueTask<int> EchoAsync(byte[] payload)
    {
      BinaryPrimitives.WriteInt32BigEndian(Buffer, payload.Length);
      payload.CopyTo(Buffer, 4);
      await Stream.WriteAsync(Buffer.AsMemory(0, 4 + payload.Length));
      await Stream.ReadExactlyAsync(Buffer.AsMemory(0, 4));
      var length = BinaryPrimitives.ReadInt32BigEndian(Buffer);
      await Stream.ReadExactlyAsync(Buffer.AsMemory(4, length));
      return length;
    }

    public async ValueTask DisposeAsync()
    {
      client.Dispose();
      listener.Stop();
      await serverTask;
    }
  }
}

static class PureGateSocket
{
  public static async Task<IEchoClient> StartAsync(byte[] payload)
  {
    var listener = new SocketListener(new SocketFabricOptions { NodeId = "server" });
    await listener.StartAsync(new IPEndPoint(IPAddress.Loopback, 0), new EchoHandler());
    var peer = await new SocketConnector(new SocketFabricOptions { NodeId = "client" })
        .ConnectAsync(listener.EndPoint, new EchoHandler());
    return new Client(peer, listener);
  }

  sealed class EchoHandler : PureGate.Socket.Handling.ISocketPeerHandler
  {
    public ValueTask<SocketResponse> HandleRequestAsync(SocketPeer peer, SocketRequest request, CancellationToken cancellationToken)
    {
      return ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload });
    }
  }

  sealed class Client(SocketPeer peer, SocketListener listener) : IEchoClient
  {
    public async ValueTask<int> EchoAsync(byte[] payload)
    {
      var result = await peer.RequestAsync("bench", "echo", payload, TimeSpan.FromSeconds(30));
      return result.Succeeded ? result.Payload.Length : -1;
    }

    public async ValueTask DisposeAsync()
    {
      await peer.DisposeAsync();
      await listener.DisposeAsync();
    }
  }
}

static class GrpcBench
{
  public static async Task<IEchoClient> StartAsync(byte[] payload)
  {
    var port = Ports.Free();
    var builder = WebApplication.CreateSlimBuilder();
    builder.Logging.ClearProviders();
    builder.WebHost.ConfigureKestrel(kestrel => kestrel.Listen(IPAddress.Loopback, port, o => o.Protocols = HttpProtocols.Http2));
    builder.Services.AddGrpc();
    var app = builder.Build();
    app.MapGrpcService<EchoService>();
    await app.StartAsync();

    var channel = GrpcChannel.ForAddress($"http://127.0.0.1:{port}");
    var client = new Echo.EchoClient(channel);
    var request = new Blob { Data = ByteString.CopyFrom(payload) };
    await client.EchoAsync(request);
    return new Client(client, request, channel, app);
  }

  sealed class EchoService : Echo.EchoBase
  {
    public override Task<Blob> Echo(Blob request, ServerCallContext context) => Task.FromResult(request);
  }

  sealed class Client(Echo.EchoClient client, Blob request, GrpcChannel channel, WebApplication app) : IEchoClient
  {
    public async ValueTask<int> EchoAsync(byte[] payload) => (await client.EchoAsync(request)).Data.Length;

    public async ValueTask DisposeAsync()
    {
      channel.Dispose();
      await app.StopAsync();
      await app.DisposeAsync();
    }
  }
}

static class SignalRBench
{
  public static async Task<IEchoClient> StartAsync(byte[] payload)
  {
    var port = Ports.Free();
    var builder = WebApplication.CreateSlimBuilder();
    builder.Logging.ClearProviders();
    builder.WebHost.ConfigureKestrel(kestrel => kestrel.Listen(IPAddress.Loopback, port));
    // SignalR rejects messages over 32 KB by default; lifted so --payload can go higher.
    builder.Services.AddSignalR(hub => hub.MaximumReceiveMessageSize = null).AddMessagePackProtocol();
    var app = builder.Build();
    app.MapHub<EchoHub>("/echo");
    await app.StartAsync();

    var connection = new HubConnectionBuilder()
        .WithUrl($"http://127.0.0.1:{port}/echo")
        .AddMessagePackProtocol()
        .Build();
    await connection.StartAsync();
    return new Client(connection, app);
  }

  sealed class EchoHub : Hub
  {
    public byte[] Echo(byte[] data) => data;
  }

  sealed class Client(HubConnection connection, WebApplication app) : IEchoClient
  {
    public async ValueTask<int> EchoAsync(byte[] payload) => (await connection.InvokeAsync<byte[]>("Echo", payload)).Length;

    public async ValueTask DisposeAsync()
    {
      await connection.DisposeAsync();
      await app.StopAsync();
      await app.DisposeAsync();
    }
  }
}

static class StreamJsonRpcBench
{
  public static async Task<IEchoClient> StartAsync(byte[] payload)
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var accept = listener.AcceptTcpClientAsync();
    var tcp = new TcpClient { NoDelay = true };
    await tcp.ConnectAsync((IPEndPoint)listener.LocalEndpoint);
    var serverTcp = await accept;
    serverTcp.NoDelay = true;

    var serverStream = serverTcp.GetStream();
    var server = new JsonRpc(new LengthHeaderMessageHandler(serverStream, serverStream, new MessagePackFormatter()), new EchoTarget());
    server.StartListening();

    var clientStream = tcp.GetStream();
    var client = new JsonRpc(new LengthHeaderMessageHandler(clientStream, clientStream, new MessagePackFormatter()));
    client.StartListening();
    return new Client(client, server, tcp, serverTcp, listener);
  }

  sealed class EchoTarget
  {
    public byte[] Echo(byte[] data) => data;
  }

  sealed class Client(JsonRpc client, JsonRpc server, TcpClient tcp, TcpClient serverTcp, TcpListener listener) : IEchoClient
  {
    public async ValueTask<int> EchoAsync(byte[] payload) => (await client.InvokeAsync<byte[]>("Echo", payload)).Length;

    public ValueTask DisposeAsync()
    {
      client.Dispose();
      server.Dispose();
      tcp.Dispose();
      serverTcp.Dispose();
      listener.Stop();
      return ValueTask.CompletedTask;
    }
  }
}

static class Ports
{
  public static int Free()
  {
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
  }
}
