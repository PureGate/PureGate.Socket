using PureGate.Socket.Observability;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

// Fast detection of half-open connections. A connection whose remote vanished without closing it (power
// loss, network partition) looks healthy until the idle timeout, which takes three heartbeats. A request
// timeout is the earliest hint: if nothing at all has arrived from the remote since the request was sent,
// a ping is sent, and if nothing arrives within LivenessProbeTimeout either, the connection is closed.
// Any frame counts as proof of life, not just the pong, and a remote that is merely slow keeps sending
// heartbeats and responses, so it is never closed by this. At most one probe runs at a time.
public sealed partial class SocketPeer
{
  const long LivenessProbeMessageId = -2;
  readonly Lock LivenessSync = new();
  Task<bool> LivenessProbe;

  // True if the remote has been heard from since sinceTicks (Environment.TickCount64), or answers a probe
  // in time. False means the connection was found dead and has been closed.
  internal Task<bool> CheckLivenessAsync(long sinceTicks)
  {
    if (State == SocketPeerState.Closed)
      return Task.FromResult(false);
    if (Volatile.Read(ref LastReceivedTicks) > sinceTicks)
      return Task.FromResult(true);

    lock (LivenessSync)
    {
      if (LivenessProbe is null || LivenessProbe.IsCompleted)
        LivenessProbe = RunLivenessProbeAsync();
      return LivenessProbe;
    }
  }

  async Task<bool> RunLivenessProbeAsync()
  {
    var started = Environment.TickCount64;
    SendControlInBackground(new SocketFrame
    {
      MessageId = LivenessProbeMessageId,
      Kind = SocketMessageKind.Ping
    });

    var deadline = started + (long)Options.LivenessProbeTimeout.TotalMilliseconds;
    try
    {
      while (Environment.TickCount64 < deadline)
      {
        if (Volatile.Read(ref LastReceivedTicks) >= started)
          return true;
        await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(20, Math.Max(1, deadline - Environment.TickCount64))), Lifetime.Token);
      }
    }
    catch (OperationCanceledException)
    {
      return false;
    }

    if (Volatile.Read(ref LastReceivedTicks) >= started)
      return true;

    SocketMetrics.LivenessFailed();
    await CloseAsync(new SocketProtocolException(SocketProtocolError.Timeout, "The remote did not answer a liveness probe."));
    return false;
  }
}
