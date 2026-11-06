using System.Buffers;
using MessagePack;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// The MessagePack codec exactly as the README documents it, so the snippet stays correct.
public sealed class MessagePackSocketMessageCodec : ISocketMessageCodec
{
  public static readonly MessagePackSocketMessageCodec Shared = new();

  public void Serialize<T>(T value, IBufferWriter<byte> output) => MessagePackSerializer.Serialize(output, value);

  public T Deserialize<T>(ReadOnlySequence<byte> payload) => MessagePackSerializer.Deserialize<T>(payload);
}

[MessagePackObject]
public sealed class PackedOrder
{
  [Key(0)] public int Id { get; set; }
  [Key(1)] public string Customer { get; set; }
  [Key(2)] public List<string> Lines { get; set; }
}

// The documented MessagePack codec: typed calls end to end, and every pooled buffer returned.
public sealed partial class SocketFabricTests
{
  [Test]
  [Category("Codec")]
  public async Task MessagePackCodec_CarriesTypedRequestsEventsAndStreams()
  {
    var codec = MessagePackSocketMessageCodec.Shared;
    var events = new TaskCompletionSource<PackedOrder>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", Codec = codec },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) =>
          {
            var order = request.ReadPayload<PackedOrder>(peer.Codec);
            order.Lines.Add("echoed");
            return ValueTask.FromResult(SocketResponse.Success(peer.Codec, order));
          },
          OnEvent = (peer, socketEvent, _) =>
          {
            events.TrySetResult(socketEvent.ReadPayload<PackedOrder>(peer.Codec));
            return ValueTask.CompletedTask;
          },
          OnStream = (peer, request, _) =>
              ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(request.ReadPayload<PackedOrder>(peer.Codec).Id)))
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", Codec = codec });
    var baseline = SocketPayload.OutstandingPooledBuffers;

    var reply = await client.RequestAsync<PackedOrder, PackedOrder>("orders", "echo", new PackedOrder { Id = 7, Customer = "Grüße", Lines = ["a", "b"] });
    await client.SendEventAsync("orders", "created", new PackedOrder { Id = 8, Customer = "event", Lines = [] });
    long streamed;
    await using (var stream = await client.OpenStreamAsync("orders", "export", new PackedOrder { Id = 50_000, Customer = "stream", Lines = [] }))
      streamed = await ReadPatternAsync(stream);

    Assert.That(reply.Id, Is.EqualTo(7));
    Assert.That(reply.Customer, Is.EqualTo("Grüße"));
    Assert.That(reply.Lines, Is.EqualTo(new[] { "a", "b", "echoed" }));
    Assert.That((await events.Task.WaitAsync(TimeSpan.FromSeconds(5))).Id, Is.EqualTo(8));
    Assert.That(streamed, Is.EqualTo(50_000));
    await WaitUntilAsync(() => SocketPayload.OutstandingPooledBuffers <= baseline, TimeSpan.FromSeconds(5));
    Assert.That(SocketPayload.OutstandingPooledBuffers, Is.LessThanOrEqualTo(baseline), "a pooled payload was not returned");
  }

  [Test]
  [Category("Codec")]
  public void MessagePackCodec_RoundTripsThroughTheCodecInterface()
  {
    var codec = MessagePackSocketMessageCodec.Shared;
    var bytes = Serialization.SocketMessageCodecExtensions.Serialize(codec, new PackedOrder { Id = 3, Customer = "x", Lines = ["1"] });
    var order = Serialization.SocketMessageCodecExtensions.Deserialize<PackedOrder>(codec, bytes);

    Assert.That(order.Id, Is.EqualTo(3));
    Assert.That(order.Lines, Is.EqualTo(new[] { "1" }));
  }
}
