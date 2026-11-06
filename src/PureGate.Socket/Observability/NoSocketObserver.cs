namespace PureGate.Socket.Observability;

public sealed class NoSocketObserver : ISocketObserver
{
  public static readonly NoSocketObserver Shared = new();
}
