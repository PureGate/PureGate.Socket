using System.Net;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Observability;

public interface ISocketObserver
{
  void PeerConnected(SocketPeer peer)
  {
  }

  void PeerDisconnected(SocketPeer peer, Exception exception)
  {
  }

  void PeerAcceptFailed(IPEndPoint remoteEndPoint, Exception exception)
  {
  }

  void ListenerAcceptFailed(IPEndPoint localEndPoint, Exception exception)
  {
  }

  void ClientConnectStarted(EndPoint remoteEndPoint)
  {
  }

  void ClientConnectFailed(EndPoint remoteEndPoint, Exception exception)
  {
  }

  void ClientReconnectScheduled(EndPoint remoteEndPoint, TimeSpan delay)
  {
  }

  void ClientKeepConnectedLoopFailed(EndPoint remoteEndPoint, Exception exception)
  {
  }

  void FrameReceived(SocketPeer peer, SocketMessageKind kind, int payloadBytes)
  {
  }

  void FrameSent(SocketPeer peer, SocketMessageKind kind, int payloadBytes)
  {
  }

  void RequestStarted(SocketPeer peer, string service, string operation)
  {
  }

  void RequestCompleted(SocketPeer peer, string service, string operation, bool succeeded, string errorCode)
  {
  }

  // A request, stream or event handler threw. The connection stays open; this is the only place the
  // full exception is reported, because the remote peer only receives a sanitized message.
  void HandlerFailed(SocketPeer peer, string service, string operation, Exception exception)
  {
  }
}
