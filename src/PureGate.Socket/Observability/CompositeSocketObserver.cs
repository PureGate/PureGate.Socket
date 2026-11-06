using System.Net;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Observability;

public sealed class CompositeSocketObserver : ISocketObserver
{
  readonly ISocketObserver[] Observers;

  public CompositeSocketObserver(params ISocketObserver[] observers)
  {
    Observers = observers
        ?.Where(observer => observer is not null)
        .ToArray()
        ?? [];
  }

  public void PeerConnected(SocketPeer peer)
  {
    foreach (var observer in Observers)
      Try(() => observer.PeerConnected(peer));
  }

  public void PeerDisconnected(SocketPeer peer, Exception exception)
  {
    foreach (var observer in Observers)
      Try(() => observer.PeerDisconnected(peer, exception));
  }

  public void PeerAcceptFailed(IPEndPoint remoteEndPoint, Exception exception)
  {
    foreach (var observer in Observers)
      Try(() => observer.PeerAcceptFailed(remoteEndPoint, exception));
  }

  public void ListenerAcceptFailed(IPEndPoint localEndPoint, Exception exception)
  {
    foreach (var observer in Observers)
      Try(() => observer.ListenerAcceptFailed(localEndPoint, exception));
  }

  public void ClientConnectStarted(IPEndPoint remoteEndPoint)
  {
    foreach (var observer in Observers)
      Try(() => observer.ClientConnectStarted(remoteEndPoint));
  }

  public void ClientConnectFailed(IPEndPoint remoteEndPoint, Exception exception)
  {
    foreach (var observer in Observers)
      Try(() => observer.ClientConnectFailed(remoteEndPoint, exception));
  }

  public void ClientReconnectScheduled(IPEndPoint remoteEndPoint, TimeSpan delay)
  {
    foreach (var observer in Observers)
      Try(() => observer.ClientReconnectScheduled(remoteEndPoint, delay));
  }

  public void ClientKeepConnectedLoopFailed(IPEndPoint remoteEndPoint, Exception exception)
  {
    foreach (var observer in Observers)
      Try(() => observer.ClientKeepConnectedLoopFailed(remoteEndPoint, exception));
  }

  public void FrameReceived(SocketPeer peer, SocketMessageKind kind, int payloadBytes)
  {
    foreach (var observer in Observers)
      Try(() => observer.FrameReceived(peer, kind, payloadBytes));
  }

  public void FrameSent(SocketPeer peer, SocketMessageKind kind, int payloadBytes)
  {
    foreach (var observer in Observers)
      Try(() => observer.FrameSent(peer, kind, payloadBytes));
  }

  public void RequestStarted(SocketPeer peer, string service, string operation)
  {
    foreach (var observer in Observers)
      Try(() => observer.RequestStarted(peer, service, operation));
  }

  public void RequestCompleted(SocketPeer peer, string service, string operation, bool succeeded, string errorCode)
  {
    foreach (var observer in Observers)
      Try(() => observer.RequestCompleted(peer, service, operation, succeeded, errorCode));
  }

  static void Try(Action action)
  {
    try
    {
      action();
    }
    catch
    {
    }
  }
}
