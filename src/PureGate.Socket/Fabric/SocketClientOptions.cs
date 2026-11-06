namespace PureGate.Socket.Fabric;

public sealed class SocketClientOptions
{
  static readonly TimeSpan MinimumReconnectDelay = TimeSpan.FromMilliseconds(10);
  static readonly TimeSpan MaximumReconnectDelay = TimeSpan.FromMinutes(5);

  public static readonly SocketClientOptions Default = new();

  public bool KeepConnected { get; init; }
  public TimeSpan ReconnectInitialDelay { get; init; } = TimeSpan.FromMilliseconds(100);
  public TimeSpan ReconnectMaxDelay { get; init; } = TimeSpan.FromSeconds(10);
  public double ReconnectJitterRatio { get; init; } = 0.2;

  internal SocketClientOptions Normalize()
  {
    var initialDelay = ReconnectInitialDelay <= TimeSpan.Zero
        ? TimeSpan.FromMilliseconds(100)
        : ReconnectInitialDelay;
    if (initialDelay < MinimumReconnectDelay)
      initialDelay = MinimumReconnectDelay;
    if (initialDelay > MaximumReconnectDelay)
      initialDelay = MaximumReconnectDelay;

    var maxDelay = ReconnectMaxDelay <= TimeSpan.Zero
        ? TimeSpan.FromSeconds(10)
        : ReconnectMaxDelay;
    if (maxDelay < initialDelay)
      maxDelay = initialDelay;
    if (maxDelay > MaximumReconnectDelay)
      maxDelay = MaximumReconnectDelay;

    return new SocketClientOptions
    {
      KeepConnected = KeepConnected,
      ReconnectInitialDelay = initialDelay,
      ReconnectMaxDelay = maxDelay,
      ReconnectJitterRatio = double.IsFinite(ReconnectJitterRatio)
          ? Math.Clamp(ReconnectJitterRatio, 0, 1)
          : 0.2
    };
  }
}
