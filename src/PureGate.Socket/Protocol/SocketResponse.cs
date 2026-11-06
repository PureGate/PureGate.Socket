using System.Buffers;
using PureGate.Socket.Serialization;

namespace PureGate.Socket.Protocol;

public sealed class SocketResponse
{
  ReadOnlyMemory<byte> PayloadValue = ReadOnlyMemory<byte>.Empty;
  DeferredPayload Deferred;
  byte[] Materialized;

  public bool Succeeded { get; init; }
  public string ErrorCode { get; init; }
  public string ErrorMessage { get; init; }

  // A response made with Success(codec, value) is serialized when it is sent, straight into a pooled
  // buffer. Reading Payload first serializes it into an array instead (once).
  public ReadOnlyMemory<byte> Payload
  {
    get => Deferred is null ? PayloadValue : Materialize();
    init
    {
      PayloadValue = value;
      Deferred = null;
    }
  }

  public static SocketResponse Success()
  {
    return new SocketResponse { Succeeded = true };
  }

  // The value is serialized when the response is sent, so it must not change after the handler returns.
  public static SocketResponse Success<T>(ISocketMessageCodec codec, T payload)
  {
    ArgumentNullException.ThrowIfNull(codec);
    return new SocketResponse
    {
      Succeeded = true,
      Deferred = new DeferredPayload<T>(codec, payload)
    };
  }

  public static SocketResponse Failed(string errorCode, string errorMessage)
  {
    return new SocketResponse
    {
      Succeeded = false,
      ErrorCode = errorCode ?? SocketProtocolError.HandlerFailed,
      ErrorMessage = errorMessage ?? string.Empty
    };
  }

  // The payload to send. A deferred payload not yet read is serialized into a pooled buffer, returned in
  // owned, which the caller disposes once the frame has been written. Throws what the codec throws.
  internal ReadOnlyMemory<byte> RentPayload(out SocketPayload owned)
  {
    owned = SocketPayload.Empty;
    var deferred = Deferred;
    if (deferred is null)
      return PayloadValue;

    var materialized = Volatile.Read(ref Materialized);
    if (materialized is not null)
      return materialized;

    owned = deferred.Serialize();
    return owned.Memory;
  }

  // The payload as an array: the serialized array itself for a typed response, otherwise a copy.
  internal byte[] PayloadArray()
  {
    var payload = Payload;
    return System.Runtime.InteropServices.MemoryMarshal.TryGetArray(payload, out var segment)
        && segment.Offset == 0
        && segment.Count == segment.Array.Length
        && Deferred is not null
            ? segment.Array
            : payload.ToArray();
  }

  ReadOnlyMemory<byte> Materialize()
  {
    var materialized = Volatile.Read(ref Materialized);
    if (materialized is not null)
      return materialized;

    var bytes = Deferred.SerializeToArray();
    return Interlocked.CompareExchange(ref Materialized, bytes, null) ?? bytes;
  }

  abstract class DeferredPayload
  {
    public abstract SocketPayload Serialize();
    public abstract byte[] SerializeToArray();
  }

  sealed class DeferredPayload<T>(ISocketMessageCodec codec, T value) : DeferredPayload
  {
    public override SocketPayload Serialize()
    {
      return SocketPayloadWriter.Serialize(codec, value);
    }

    public override byte[] SerializeToArray()
    {
      return SocketPayloadWriter.SerializeToArray(codec, value);
    }
  }
}
