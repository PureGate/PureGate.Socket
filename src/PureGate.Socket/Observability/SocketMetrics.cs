using System.Diagnostics;
using System.Diagnostics.Metrics;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Observability;

// Metrics through System.Diagnostics.Metrics, under the meter named MeterName. Collect them with
// OpenTelemetry (AddMeter("PureGate.Socket")), dotnet-counters, or any MeterListener.
//
// Nothing is measured until a listener subscribes: each recording checks the instrument's Enabled flag
// first, and the gauges and byte totals are computed from per-connection state only when collected.
//
// Counters and histograms:
//   puregate.socket.connections.closed        {connection}  error.type when the connection failed
//   puregate.socket.handshake.failures        {handshake}   role (client, server), error.type
//   puregate.socket.client.request.duration   s             rpc.service, rpc.method, error.type
//   puregate.socket.server.request.duration   s             rpc.service, rpc.method, error.type
//   puregate.socket.events.sent               {event}       rpc.service, rpc.method
//   puregate.socket.events.received           {event}       rpc.service, rpc.method
//   puregate.socket.memory.rejections         {message}     message.kind
//   puregate.socket.flow_control.waits        {wait}        a sender waited for message credit
//   puregate.socket.liveness.failures         {connection}  closed after an unanswered liveness probe
// Observed when collected, summed over open connections:
//   puregate.socket.connections               {connection}
//   puregate.socket.bytes.sent, .received     By            frames on the wire, over every connection opened
//   puregate.socket.requests.in_flight        {request}     this side's requests and streams awaiting the remote
//   puregate.socket.handlers.queued           {message}     received messages waiting for a handler
//   puregate.socket.handlers.active           {worker}
//   puregate.socket.streams.active            {stream}      direction (in, out)
//   puregate.socket.memory.used               By            held against connection memory budgets
public static class SocketMetrics
{
  public const string MeterName = "PureGate.Socket";

  // Latency buckets from 50 µs to 10 s; the OpenTelemetry defaults start at 5 ms, too coarse for this.
  static readonly IReadOnlyList<double> DurationBuckets =
      [0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

  static readonly Meter Meter = new(MeterName, typeof(SocketMetrics).Assembly.GetName().Version?.ToString());
  static readonly Lock Sync = new();
  static readonly HashSet<SocketPeer> Live = [];
  static long ClosedBytesSent;
  static long ClosedBytesReceived;

  static readonly Counter<long> ConnectionsClosed = Meter.CreateCounter<long>(
      "puregate.socket.connections.closed", "{connection}", "Connections closed.");
  static readonly Counter<long> HandshakeFailures = Meter.CreateCounter<long>(
      "puregate.socket.handshake.failures", "{handshake}", "Connections that failed or were refused before the handshake completed.");
  static readonly Histogram<double> ClientRequestDuration = Meter.CreateHistogram(
      "puregate.socket.client.request.duration", "s", "Time from sending a request to its result.",
      advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = DurationBuckets });
  static readonly Histogram<double> ServerRequestDuration = Meter.CreateHistogram(
      "puregate.socket.server.request.duration", "s", "Time a received request spent in its handler.",
      advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = DurationBuckets });
  static readonly Counter<long> EventsSent = Meter.CreateCounter<long>(
      "puregate.socket.events.sent", "{event}", "Events sent.");
  static readonly Counter<long> EventsReceived = Meter.CreateCounter<long>(
      "puregate.socket.events.received", "{event}", "Events handled.");
  static readonly Counter<long> MemoryRejections = Meter.CreateCounter<long>(
      "puregate.socket.memory.rejections", "{message}", "Messages refused because they did not fit the connection's memory budget.");
  static readonly Counter<long> CreditWaits = Meter.CreateCounter<long>(
      "puregate.socket.flow_control.waits", "{wait}", "Sends that waited because the remote's handler queue was full.");
  static readonly Counter<long> LivenessFailures = Meter.CreateCounter<long>(
      "puregate.socket.liveness.failures", "{connection}", "Connections closed because the remote did not answer a liveness probe.");

  static SocketMetrics()
  {
    Meter.CreateObservableUpDownCounter("puregate.socket.connections", Count, "{connection}", "Open connections.");
    Meter.CreateObservableCounter("puregate.socket.bytes.sent", BytesSent, "By", "Bytes of frames sent.");
    Meter.CreateObservableCounter("puregate.socket.bytes.received", BytesReceived, "By", "Bytes of frames received.");
    Meter.CreateObservableUpDownCounter("puregate.socket.requests.in_flight", () => Sum(d => d.InFlight), "{request}", "Requests and streams awaiting the remote.");
    Meter.CreateObservableUpDownCounter("puregate.socket.handlers.queued", () => Sum(d => d.QueuedHandlers), "{message}", "Received messages waiting for a handler.");
    Meter.CreateObservableUpDownCounter("puregate.socket.handlers.active", () => Sum(d => d.ActiveHandlerWorkers), "{worker}", "Handler workers running.");
    Meter.CreateObservableUpDownCounter("puregate.socket.streams.active", Streams, "{stream}", "Open streams.");
    Meter.CreateObservableUpDownCounter("puregate.socket.memory.used", () => Sum(d => d.MemoryInUse), "By", "Memory held against connection memory budgets.");
  }

  internal static void Opened(SocketPeer peer)
  {
    lock (Sync)
      Live.Add(peer);
  }

  internal static void Closed(SocketPeer peer, Exception exception)
  {
    lock (Sync)
    {
      if (!Live.Remove(peer))
        return;
      var diagnostics = peer.Diagnostics;
      ClosedBytesSent += diagnostics.BytesSent;
      ClosedBytesReceived += diagnostics.BytesReceived;
    }

    if (!ConnectionsClosed.Enabled)
      return;
    if (exception is null)
      ConnectionsClosed.Add(1);
    else
      ConnectionsClosed.Add(1, new KeyValuePair<string, object>("error.type", ErrorType(exception)));
  }

  internal static void HandshakeFailed(string role, Exception exception)
  {
    if (HandshakeFailures.Enabled)
    {
      HandshakeFailures.Add(
          1,
          new KeyValuePair<string, object>("role", role),
          new KeyValuePair<string, object>("error.type", ErrorType(exception)));
    }
  }

  // A timestamp to pass to the matching Completed call, or 0 when nobody is listening.
  internal static long ClientRequestStarted()
  {
    return ClientRequestDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
  }

  internal static void ClientRequestCompleted(string service, string operation, long started, string errorCode)
  {
    if (started != 0)
      RecordDuration(ClientRequestDuration, service, operation, started, errorCode);
  }

  internal static long ServerRequestStarted()
  {
    return ServerRequestDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
  }

  internal static void ServerRequestCompleted(string service, string operation, long started, string errorCode)
  {
    if (started != 0)
      RecordDuration(ServerRequestDuration, service, operation, started, errorCode);
  }

  internal static void EventSent(string service, string operation)
  {
    if (EventsSent.Enabled)
      EventsSent.Add(1, Method(service, operation));
  }

  internal static void EventReceived(string service, string operation)
  {
    if (EventsReceived.Enabled)
      EventsReceived.Add(1, Method(service, operation));
  }

  internal static void MemoryRejected(SocketMessageKind kind)
  {
    if (MemoryRejections.Enabled)
      MemoryRejections.Add(1, new KeyValuePair<string, object>("message.kind", KindName(kind)));
  }

  internal static void CreditWaited()
  {
    if (CreditWaits.Enabled)
      CreditWaits.Add(1);
  }

  internal static void LivenessFailed()
  {
    if (LivenessFailures.Enabled)
      LivenessFailures.Add(1);
  }

  static void RecordDuration(Histogram<double> histogram, string service, string operation, long started, string errorCode)
  {
    var tags = Method(service, operation);
    if (!string.IsNullOrEmpty(errorCode))
      tags.Add("error.type", errorCode);
    histogram.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags);
  }

  static TagList Method(string service, string operation)
  {
    return new TagList
    {
      { "rpc.service", service },
      { "rpc.method", operation }
    };
  }

  static string ErrorType(Exception exception)
  {
    return exception is SocketProtocolException protocol
        ? protocol.ErrorCode
        : exception.GetType().FullName;
  }

  static string KindName(SocketMessageKind kind)
  {
    return kind switch
    {
      SocketMessageKind.Request => "request",
      SocketMessageKind.Response => "response",
      SocketMessageKind.Event => "event",
      SocketMessageKind.StreamRequest => "stream_request",
      SocketMessageKind.StreamItem => "stream_item",
      _ => "other"
    };
  }

  static SocketPeer[] Snapshot()
  {
    lock (Sync)
      return [.. Live];
  }

  static int Count()
  {
    lock (Sync)
      return Live.Count;
  }

  static long Sum(Func<SocketPeerDiagnostics, long> value)
  {
    long total = 0;
    foreach (var peer in Snapshot())
      total += value(peer.Diagnostics);
    return total;
  }

  static IEnumerable<Measurement<long>> Streams()
  {
    long incoming = 0;
    long outgoing = 0;
    foreach (var peer in Snapshot())
    {
      var diagnostics = peer.Diagnostics;
      incoming += diagnostics.PendingStreams;
      outgoing += diagnostics.OutgoingStreams;
    }

    return
    [
      new Measurement<long>(incoming, new KeyValuePair<string, object>("direction", "in")),
      new Measurement<long>(outgoing, new KeyValuePair<string, object>("direction", "out"))
    ];
  }

  // Totals over every connection ever opened. Taken under the lock that moves a closing connection's
  // totals into the closed sum, so the counters never go backwards.
  static long BytesSent()
  {
    lock (Sync)
    {
      var total = ClosedBytesSent;
      foreach (var peer in Live)
        total += peer.Diagnostics.BytesSent;
      return total;
    }
  }

  static long BytesReceived()
  {
    lock (Sync)
    {
      var total = ClosedBytesReceived;
      foreach (var peer in Live)
        total += peer.Diagnostics.BytesReceived;
      return total;
    }
  }
}
