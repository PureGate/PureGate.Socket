namespace PureGate.Socket.Fabric;

public sealed class SocketClientRequestOptions
{
  public static readonly SocketClientRequestOptions Default = new();

  public bool Idempotent { get; init; }
  public string IdempotencyKey { get; init; }
  public int MaxAttempts { get; init; } = 1;
  public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(50);

  internal SocketClientRequestOptions Normalize()
  {
    return new SocketClientRequestOptions
    {
      Idempotent = Idempotent,
      IdempotencyKey = string.IsNullOrWhiteSpace(IdempotencyKey) ? string.Empty : IdempotencyKey.Trim(),
      MaxAttempts = Math.Clamp(MaxAttempts, 1, 100),
      RetryDelay = RetryDelay < TimeSpan.Zero ? TimeSpan.Zero : RetryDelay
    };
  }
}
