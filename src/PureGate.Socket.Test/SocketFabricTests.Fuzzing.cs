using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using PureGate.Socket.Framing;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Test;

// Dependency-free fuzzing. Every run uses fixed seeds, so a failure reproduces exactly; the failing seed
// and input are in the assertion message.
public sealed partial class SocketFabricTests
{
  const int FuzzSeed = 20261106;

  [Test]
  [Category("Fuzz")]
  public async Task Fuzz_DecoderOnlyEverReturnsFramesOrThrowsProtocolErrors()
  {
    var corpus = await BuildFrameCorpusAsync();
    var random = new Random(FuzzSeed);
    for (var iteration = 0; iteration < 100_000; iteration++)
    {
      var input = Mutate(corpus[random.Next(corpus.Count)], random);
      try
      {
        var reader = new SocketFrameReader(new MemoryStream(input), 64 * 1024, SocketPayloadCompression.Brotli);
        for (var frames = 0; frames < 64; frames++)
        {
          var frame = await reader.ReadAsync(CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5));
          if (frame is null)
            break;
          SocketCompression.DecompressFrame(frame, 1024 * 1024);
          frame.ReleasePayload();
        }
      }
      catch (SocketProtocolException)
      {
      }
      catch (Exception ex)
      {
        Assert.Fail($"iteration {iteration}: {ex.GetType().Name}: {ex.Message}\ninput: {Convert.ToHexString(input)}");
      }
    }
  }

  [Test]
  [Category("Fuzz")]
  public async Task Fuzz_RandomFramesRoundTripExactly()
  {
    var random = new Random(FuzzSeed);
    for (var round = 0; round < 50; round++)
    {
      var frames = Enumerable.Range(0, 40).Select(_ => RandomFrame(random)).ToList();
      using var encoded = new MemoryStream();
      var writer = new SocketFrameWriter(encoded, 1024 * 1024, useMethodTable: true);
      foreach (var frame in frames)
        await writer.WriteAsync(frame, CancellationToken.None);

      // Segmented reads exercise the same decoder over pipe segment boundaries.
      Stream source = round % 2 == 0
          ? new MemoryStream(encoded.ToArray())
          : new TrickleStream(encoded.ToArray(), maxBytesPerRead: 1 + random.Next(64));
      var decoded = await ReadAllFramesAsync(source, frames.Count);
      for (var i = 0; i < frames.Count; i++)
      {
        frames[i].Service ??= string.Empty;
        frames[i].Operation ??= string.Empty;
        frames[i].IdempotencyKey ??= string.Empty;
        AssertSameFrame(decoded[i], frames[i]);
      }
    }
  }

  [Test]
  [Category("Fuzz")]
  public async Task Fuzz_MutatedTrafficAfterTheHandshakeNeverHurtsTheListener()
  {
    var corpus = await BuildFrameCorpusAsync();
    var random = new Random(FuzzSeed);
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions { NodeId = "server", MaxFrameSizeBytes = 64 * 1024 },
        new DelegateSocketPeerHandler
        {
          OnRequest = (_, request, _) => ValueTask.FromResult(new SocketResponse { Succeeded = true, Payload = request.Payload }),
          OnStream = (_, _, _) => ValueTask.FromResult(SocketStreamResponse.Success(new MemoryStream(new byte[1000])))
        });

    for (var connection = 0; connection < 150; connection++)
    {
      using var tcp = new TcpClient(AddressFamily.InterNetwork) { NoDelay = true };
      await tcp.ConnectAsync(listener.EndPoint);
      var stream = tcp.GetStream();
      await RunRawNoAuthHandshakeAsync(stream);
      try
      {
        for (var i = 0; i < 1 + random.Next(6); i++)
          await stream.WriteAsync(random.Next(3) == 0 ? corpus[random.Next(corpus.Count)] : Mutate(corpus[random.Next(corpus.Count)], random));
      }
      catch (IOException)
      {
        // The server already closed this connection; that is an acceptable outcome.
      }
    }

    // Whatever the fuzzed connections did, a well-behaved client must still be served.
    await using var client = await ConnectAsync(listener);
    var result = await client.RequestAsync("test", "after-fuzz", [1, 2, 3], TimeSpan.FromSeconds(5));
    Assert.That(result.Succeeded, Is.True, result.ErrorCode);
  }

  [Test]
  [Category("Fuzz")]
  public async Task Fuzz_GarbageBeforeTheHandshakeIsRejectedCleanly()
  {
    var random = new Random(FuzzSeed);
    var rejected = 0;
    await using var listener = await StartListenerAsync(
        new SocketFabricOptions
        {
          NodeId = "server",
          HandshakeTimeout = TimeSpan.FromSeconds(1),
          Observer = new DelegateSocketObserver { OnPeerAcceptFailed = (_, _) => Interlocked.Increment(ref rejected) }
        },
        new DelegateSocketPeerHandler { OnRequest = (_, _, _) => ValueTask.FromResult(SocketResponse.Success()) });

    var hello = EncodeHandshakeHello();
    var inputs = Enumerable.Range(0, 100).Select(connection => (connection % 3) switch
    {
      0 => RandomBytes(random, 1 + random.Next(200)),
      1 => Mutate(hello, random),
      _ => (byte[])[0x7F, 0xFF, 0xFF, 0xFF, .. RandomBytes(random, 32)]
    }).ToList();

    // In parallel: connections that send an incomplete frame only end at the handshake timeout.
    await Task.WhenAll(inputs.Select(async input =>
    {
      using var tcp = new TcpClient(AddressFamily.InterNetwork) { NoDelay = true };
      await tcp.ConnectAsync(listener.EndPoint);
      try
      {
        await tcp.GetStream().WriteAsync(input);
        var buffer = new byte[4096];
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        while (await tcp.GetStream().ReadAsync(buffer, timeout.Token) > 0)
        {
        }
      }
      catch (Exception ex) when (ex is IOException or OperationCanceledException)
      {
      }
    }));

    await using var client = await ConnectAsync(listener);
    var result = await client.RequestAsync("test", "after-fuzz", [], TimeSpan.FromSeconds(5));
    Assert.That(result.Succeeded, Is.True, result.ErrorCode);
    await WaitUntilAsync(() => Volatile.Read(ref rejected) >= 90, TimeSpan.FromSeconds(5));
    Assert.That(rejected, Is.GreaterThanOrEqualTo(90), "nearly every garbage connection should be reported as a failed accept");
  }

  // Valid encoded frames of every kind, with and without compression and method-table references.
  static async ValueTask<List<byte[]>> BuildFrameCorpusAsync()
  {
    var random = new Random(FuzzSeed + 1);
    var corpus = new List<byte[]>();
    foreach (var compression in new[] { SocketPayloadCompression.None, SocketPayloadCompression.Brotli })
    {
      using var output = new MemoryStream();
      var writer = new SocketFrameWriter(output, 1024 * 1024, compression, compressionMinPayloadBytes: 64, useMethodTable: true);
      var offsets = new List<long>();
      for (var i = 0; i < 60; i++)
      {
        offsets.Add(output.Length);
        await writer.WriteAsync(RandomFrame(random), CancellationToken.None);
      }

      var bytes = output.ToArray();
      offsets.Add(bytes.Length);
      for (var i = 0; i < offsets.Count - 1; i++)
        corpus.Add(bytes[(int)offsets[i]..(int)offsets[i + 1]]);
      // Whole streams too, so method references point at definitions earlier in the input.
      corpus.Add(bytes);
    }

    return corpus;
  }

  static SocketFrame RandomFrame(Random random)
  {
    var kinds = Enum.GetValues<SocketMessageKind>();
    var kind = kinds[random.Next(kinds.Length)];
    var frame = new SocketFrame
    {
      Kind = kind,
      MessageId = random.Next(4) == 0 ? -random.NextInt64(1, 1000) : random.NextInt64(0, long.MaxValue),
      Ok = random.Next(2) == 0,
      DeadlineMs = random.Next(3) == 0 ? random.Next(1, int.MaxValue) : 0,
      Value = random.Next(3) == 0 ? random.Next(int.MinValue, int.MaxValue) : 0,
      Payload = RandomBytes(random, random.Next(4) == 0 ? random.Next(2000) : random.Next(40))
    };
    if (random.Next(2) == 0)
    {
      frame.Service = RandomText(random, "svc", 4);
      frame.Operation = RandomText(random, "op", 4);
    }

    if (random.Next(4) == 0)
      frame.IdempotencyKey = RandomText(random, "key", 1000);
    if (random.Next(4) == 0)
    {
      frame.ErrorCode = random.Next(2) == 0 ? SocketProtocolError.Timeout : RandomText(random, "custom", 1000);
      frame.ErrorMessage = RandomText(random, "message ü ✓", 1000);
    }

    if (kind == SocketMessageKind.Chunk)
    {
      frame.OriginalKind = SocketMessageKind.Event;
      frame.ChunkIndex = random.Next(10);
      frame.ChunkCount = 10 + random.Next(10);
      frame.TotalPayloadLength = random.Next(1, 1_000_000);
    }

    if (kind == SocketMessageKind.StreamItem && random.Next(2) == 0)
      frame.Flags = SocketFrameFlags.WindowExhausted;

    return frame;
  }

  // A few names repeat on purpose, so frames reference method-table entries as well as define them.
  static string RandomText(Random random, string prefix, int variety)
  {
    return $"{prefix}-{random.Next(variety)}";
  }

  static byte[] Mutate(byte[] input, Random random)
  {
    var bytes = input.ToList();
    var mutations = 1 + random.Next(4);
    for (var m = 0; m < mutations; m++)
    {
      switch (random.Next(7))
      {
        case 0 when bytes.Count > 0:
          bytes[random.Next(bytes.Count)] ^= (byte)(1 << random.Next(8));
          break;
        case 1 when bytes.Count > 0:
          bytes[random.Next(bytes.Count)] = (byte)random.Next(256);
          break;
        case 2 when bytes.Count > 1:
          var removeAt = random.Next(bytes.Count);
          bytes.RemoveRange(removeAt, 1 + random.Next(Math.Min(8, bytes.Count - removeAt)));
          break;
        case 3:
          bytes.InsertRange(random.Next(bytes.Count + 1), RandomBytes(random, 1 + random.Next(8)));
          break;
        case 4 when bytes.Count > 0:
          // Extreme varint bytes: endless continuation or maximum values.
          var at = random.Next(bytes.Count);
          for (var i = at; i < Math.Min(bytes.Count, at + 1 + random.Next(12)); i++)
            bytes[i] = random.Next(2) == 0 ? (byte)0xFF : (byte)0x80;
          break;
        case 5 when bytes.Count >= 4:
          // Rewrite the length prefix of the first frame.
          var length = random.Next(4) switch
          {
            0 => 0,
            1 => 1,
            2 => int.MaxValue,
            _ => random.Next(0, 70_000)
          };
          var prefix = new byte[4];
          BinaryPrimitives.WriteInt32BigEndian(prefix, length);
          for (var i = 0; i < 4; i++)
            bytes[i] = prefix[i];
          break;
        case 6 when bytes.Count > 1:
          // Truncate.
          var cut = random.Next(bytes.Count);
          bytes.RemoveRange(cut, bytes.Count - cut);
          break;
      }
    }

    return [.. bytes];
  }

  static byte[] RandomBytes(Random random, int length)
  {
    var bytes = new byte[length];
    random.NextBytes(bytes);
    return bytes;
  }

  static byte[] EncodeHandshakeHello()
  {
    using var output = new MemoryStream();
    new SocketFrameWriter(output, 1024 * 1024).WriteAsync(new SocketFrame
    {
      Kind = SocketMessageKind.Event,
      Service = "socket",
      Operation = "hello",
      Payload = Serialization.JsonSocketMessageCodec.Shared.Serialize(new SocketHandshakeHello(
          SocketFabricOptions.CurrentProtocolVersion,
          "fuzzer",
          Guid.NewGuid().ToString("N")))
    }, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    return output.ToArray();
  }
}
