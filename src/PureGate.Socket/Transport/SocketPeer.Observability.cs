using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;

namespace PureGate.Socket.Transport;

public sealed partial class SocketPeer
{
  void ObservePeerConnected()
  {
    try
    {
      Options.Observer.PeerConnected(this);
    }
    catch
    {
    }
  }

  void ObservePeerDisconnected(Exception exception)
  {
    try
    {
      Options.Observer.PeerDisconnected(this, exception);
    }
    catch
    {
    }
  }

  void ObserveFrameReceived(SocketFrame frame)
  {
    try
    {
      Options.Observer.FrameReceived(this, frame.Kind, frame.PayloadLength);
    }
    catch
    {
    }
  }

  void ObserveFrameSent(SocketFrame frame)
  {
    try
    {
      Options.Observer.FrameSent(this, frame.Kind, frame.PayloadLength);
    }
    catch
    {
    }
  }

  void ObserveRequestStarted(SocketFrame frame)
  {
    try
    {
      Options.Observer.RequestStarted(this, frame.Service, frame.Operation);
    }
    catch
    {
    }
  }

  void ObserveRequestCompleted(SocketFrame frame, SocketResponse response)
  {
    try
    {
      Options.Observer.RequestCompleted(
          this,
          frame.Service,
          frame.Operation,
          response?.Succeeded == true,
          response?.ErrorCode);
    }
    catch
    {
    }
  }
}
