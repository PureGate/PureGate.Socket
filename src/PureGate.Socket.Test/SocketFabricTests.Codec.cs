using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using PureGate.Socket.Protocol;
using PureGate.Socket.Serialization;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

sealed record CodecOrder(int Id, string Name, decimal Total);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(CodecOrder))]
partial class CodecTestJsonContext : JsonSerializerContext;

// The codec API: values are serialized straight into pooled buffers and read from the received bytes in
// place, with source-generated JSON supported.
public sealed partial class SocketFabricTests
{
  [Test]
  [Category("Codec")]
  public async Task Codec_SourceGeneratedJsonCarriesTypedRequestsEventsAndStreams()
  {
    var codec = new JsonSocketMessageCodec(CodecTestJsonContext.Default);
    var events = new TaskCompletionSource<CodecOrder>(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", Codec = codec },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, request, _) =>
          {
            var order = request.ReadPayload<CodecOrder>(peer.Codec);
            return ValueTask.FromResult(SocketResponse.Success(peer.Codec, order with { Total = order.Total * 2 }));
          },
          OnEvent = (peer, socketEvent, _) =>
          {
            events.TrySetResult(socketEvent.ReadPayload<CodecOrder>(peer.Codec));
            return ValueTask.CompletedTask;
          },
          OnStream = (peer, request, _) =>
          {
            var order = request.ReadPayload<CodecOrder>(peer.Codec);
            return ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(order.Id)));
          }
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", Codec = codec });

    var doubled = await client.RequestAsync<CodecOrder, CodecOrder>("orders", "double", new CodecOrder(7, "Grüße", 10.5m));
    await client.SendEventAsync("orders", "created", new CodecOrder(8, "event", 1m));
    await using var stream = await client.OpenStreamAsync("orders", "export", new CodecOrder(100_000, "stream", 0m));

    Assert.That(doubled, Is.EqualTo(new CodecOrder(7, "Grüße", 21m)));
    Assert.That(await events.Task.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo(new CodecOrder(8, "event", 1m)));
    Assert.That(await ReadPatternAsync(stream), Is.EqualTo(100_000));
  }

  [Test]
  [Category("Codec")]
  public void Codec_JsonReadsMultiSegmentPayloads()
  {
    var codec = new JsonSocketMessageCodec(CodecTestJsonContext.Default);
    var bytes = codec.Serialize(new CodecOrder(3, "split across segments", 4.25m));
    Assert.That(Encoding.UTF8.GetString(bytes), Does.Contain("\"name\""), "web naming applies");

    var first = new Segment(bytes.AsMemory(0, bytes.Length / 2));
    var last = first.Append(bytes.AsMemory(bytes.Length / 2));
    var sequence = new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);

    Assert.That(sequence.IsSingleSegment, Is.False);
    Assert.That(codec.Deserialize<CodecOrder>(sequence), Is.EqualTo(new CodecOrder(3, "split across segments", 4.25m)));
  }

  [Test]
  [Category("Codec")]
  public void Codec_RawCodecWritesBytesAndStrings()
  {
    var codec = RawSocketMessageCodec.Shared;
    Assert.That(codec.Serialize(new byte[] { 1, 2, 3 }), Is.EqualTo(new byte[] { 1, 2, 3 }));
    Assert.That(codec.Serialize("héllo"), Is.EqualTo(Encoding.UTF8.GetBytes("héllo")));
    Assert.That(codec.Deserialize<string>(Encoding.UTF8.GetBytes("héllo")), Is.EqualTo("héllo"));
    Assert.That(codec.Deserialize<byte[]>(new byte[] { 4, 5 }), Is.EqualTo(new byte[] { 4, 5 }));
    Assert.Throws<NotSupportedException>(() => codec.Serialize(42));
  }

  [Test]
  [Category("Codec")]
  public async Task Codec_ResponseTheCodecCannotSerializeFailsTheRequestNotTheConnection()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", Codec = RawSocketMessageCodec.Shared },
        new DelegateSocketPeerHandler
        {
          // The raw codec has no encoding for an int; that only shows when the response is sent.
          OnRequest = (peer, request, _) => ValueTask.FromResult(request.Operation == "bad"
              ? SocketResponse.Success(peer.Codec, 42)
              : SocketResponse.Success(peer.Codec, "ok"))
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", Codec = RawSocketMessageCodec.Shared });

    var bad = await client.RequestAsync("test", "bad", [], TimeSpan.FromSeconds(5));
    var good = await client.RequestAsync("test", "good", [], TimeSpan.FromSeconds(5));

    Assert.That(bad.ErrorCode, Is.EqualTo(SocketProtocolError.HandlerFailed));
    Assert.That(good.Succeeded, Is.True);
    Assert.That(Encoding.UTF8.GetString(good.Payload.Span), Is.EqualTo("ok"));
  }

  [Test]
  [Category("Codec")]
  public async Task Codec_TypedResponseKeptForIdempotentRetriesIsSentIdentically()
  {
    var calls = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server" },
        new DelegateSocketPeerHandler
        {
          OnRequest = (peer, _, _) => ValueTask.FromResult(
              SocketResponse.Success(peer.Codec, new CodecOrder(Interlocked.Increment(ref calls), "once", 1m)))
        });
    await using var client = await ConnectAsync(listener);

    var first = await client.RequestAsync("orders", "create", [], TimeSpan.FromSeconds(5), idempotencyKey: "order-9");
    var retry = await client.RequestAsync("orders", "create", [], TimeSpan.FromSeconds(5), idempotencyKey: "order-9");

    Assert.That(calls, Is.EqualTo(1));
    Assert.That(retry.Payload.ToArray(), Is.EqualTo(first.Payload.ToArray()));
    Assert.That(first.ReadPayload<CodecOrder>(client.Codec), Is.EqualTo(new CodecOrder(1, "once", 1m)));
  }

  [Test]
  [Category("Codec")]
  public async Task Codec_TypedSendsReturnEveryPooledBuffer()
  {
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxFrameSizeBytes = 64 * 1024 },
        new DelegateSocketPeerHandler
        {
          OnRequest = async (peer, request, cancellationToken) =>
          {
            if (request.Operation == "slow")
              await Task.Delay(200, cancellationToken);
            return SocketResponse.Success(peer.Codec, request.ReadPayload<CodecOrder>(peer.Codec));
          },
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new PatternStream(10_000)))
        });
    await using var client = await ConnectAsync(listener, new SocketFabricOptions { NodeId = "client", MaxFrameSizeBytes = 64 * 1024 });
    var baseline = SocketPayload.OutstandingPooledBuffers;
    var large = new CodecOrder(1, new string('x', 300_000), 1m);

    for (var i = 0; i < 20; i++)
    {
      Assert.That(await client.RequestAsync<CodecOrder, CodecOrder>("orders", "echo", new CodecOrder(i, "small", i)), Is.EqualTo(new CodecOrder(i, "small", i)));
      await client.SendEventAsync("orders", "created", new CodecOrder(i, "event", i));
    }

    // Chunked, because it is larger than the frame limit.
    Assert.That((await client.RequestAsync<CodecOrder, CodecOrder>("orders", "echo", large)).Name, Has.Length.EqualTo(300_000));
    await using (var stream = await client.OpenStreamAsync("orders", "export", new CodecOrder(0, "stream", 0m)))
      Assert.That(await ReadPatternAsync(stream), Is.EqualTo(10_000));

    // A caller that gives up while its request is outstanding.
    using (var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(20)))
    {
      var cancelled = await client.RequestAsync("orders", "slow", new CodecOrder(0, "slow", 0m), cancellation.Token);
      Assert.That(cancelled.ErrorCode, Is.EqualTo(SocketProtocolError.Cancelled));
    }

    var batch = await client.RequestBatchAsync(
        [new SocketBatchItem(1, "orders", "echo", client.Codec.Serialize(new CodecOrder(5, "batched", 5m)))],
        TimeSpan.FromSeconds(5));
    Assert.That(client.Codec.Deserialize<CodecOrder>(batch[0].Payload), Is.EqualTo(new CodecOrder(5, "batched", 5m)));

    await WaitUntilAsync(() => SocketPayload.OutstandingPooledBuffers <= baseline, TimeSpan.FromSeconds(5));
    Assert.That(SocketPayload.OutstandingPooledBuffers, Is.LessThanOrEqualTo(baseline), "a pooled payload was not returned");
  }

  sealed class Segment : ReadOnlySequenceSegment<byte>
  {
    public Segment(ReadOnlyMemory<byte> memory)
    {
      Memory = memory;
    }

    public Segment Append(ReadOnlyMemory<byte> memory)
    {
      var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
      Next = next;
      return next;
    }
  }
}
