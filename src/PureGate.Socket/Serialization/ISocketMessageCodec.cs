using System.Buffers;

namespace PureGate.Socket.Serialization;

// Turns message values into payload bytes and back.
//
// Serialize writes into a buffer the connection supplies (pooled, and returned once the frame is sent),
// so a typed send allocates no payload array. Deserialize reads the received bytes in place. Both run
// synchronously on the caller's thread; an implementation must not keep output or payload after it
// returns.
public interface ISocketMessageCodec
{
  void Serialize<T>(T value, IBufferWriter<byte> output);
  T Deserialize<T>(ReadOnlySequence<byte> payload);
}
