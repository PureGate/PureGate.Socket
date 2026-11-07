// Stand-ins for newer .NET APIs on the older frameworks the library targets. Each is compiled only where the
// real API is missing, so .NET 10 builds use the framework's own.

#if !NET9_0_OR_GREATER
// System.Threading.Lock is .NET 9 and later. Before it, lock statements on a plain object use Monitor, with the
// same semantics.
global using Lock = object;
#endif

namespace PureGate.Socket;

#if !NET8_0_OR_GREATER
static class CancellationTokenSourceCompatibility
{
  // CancellationTokenSource.CancelAsync is .NET 8 and later: it runs the callbacks off the calling thread and
  // completes when they have run.
  public static Task CancelAsync(this CancellationTokenSource source) => Task.Run(source.Cancel);
}
#endif

#if !NET7_0_OR_GREATER
static class StopwatchCompatibility
{
  extension(System.Diagnostics.Stopwatch)
  {
    // Stopwatch.GetElapsedTime is .NET 7 and later.
    public static TimeSpan GetElapsedTime(long startingTimestamp) =>
      new((long)((System.Diagnostics.Stopwatch.GetTimestamp() - startingTimestamp) * ((double)TimeSpan.TicksPerSecond / System.Diagnostics.Stopwatch.Frequency)));
  }
}
#endif
