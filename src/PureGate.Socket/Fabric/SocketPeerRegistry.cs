using System.Collections.Concurrent;
using PureGate.Socket.Observability;
using PureGate.Socket.Protocol;
using PureGate.Socket.Transport;

namespace PureGate.Socket.Fabric;

public sealed class SocketPeerRegistry : ISocketObserver
{
  readonly ConcurrentDictionary<string, NodePeerSet> Peers = new(StringComparer.Ordinal);

  public int Count => Peers.Values.Sum(nodePeers => nodePeers.Count);

  public IReadOnlyCollection<string> NodeIds
  {
    get
    {
      return Peers
          .Where(item => item.Value.Count > 0)
          .Select(item => item.Key)
          .ToArray();
    }
  }

  public IReadOnlyCollection<SocketPeer> Snapshot()
  {
    var snapshots = Peers.Values
        .Select(nodePeers => nodePeers.Snapshot())
        .Where(snapshot => snapshot.Length > 0)
        .ToArray();
    if (snapshots.Length == 0)
      return [];

    var count = snapshots.Sum(snapshot => snapshot.Length);
    var result = new SocketPeer[count];
    var offset = 0;
    foreach (var snapshot in snapshots)
    {
      Array.Copy(snapshot, 0, result, offset, snapshot.Length);
      offset += snapshot.Length;
    }

    return result;
  }

  public IReadOnlyCollection<SocketPeer> Snapshot(string nodeId)
  {
    if (string.IsNullOrWhiteSpace(nodeId))
      return [];

    return Peers.TryGetValue(nodeId, out var nodePeers)
        ? nodePeers.PublicSnapshot()
        : [];
  }

  public bool TryGet(string nodeId, out SocketPeer peer)
  {
    return TryGet(nodeId, SocketPeerSelectionPolicy.RoundRobin, out peer);
  }

  public bool TryGet(string nodeId, SocketPeerSelectionPolicy selectionPolicy, out SocketPeer peer)
  {
    if (string.IsNullOrWhiteSpace(nodeId))
    {
      peer = null;
      return false;
    }

    if (Peers.TryGetValue(nodeId, out var nodePeers))
    {
      return nodePeers.TrySelect(selectionPolicy, out peer);
    }

    peer = null;
    return false;
  }

  public SocketPeer Get(string nodeId)
  {
    return Get(nodeId, SocketPeerSelectionPolicy.RoundRobin);
  }

  public SocketPeer Get(string nodeId, SocketPeerSelectionPolicy selectionPolicy)
  {
    if (TryGet(nodeId, selectionPolicy, out var peer))
      return peer;

    throw new SocketProtocolException(SocketProtocolError.ConnectionClosed, $"Peer '{nodeId}' is not connected.");
  }

  public async ValueTask<SocketRequestResult> RequestAsync<TRequest>(
      string nodeId,
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId).RequestAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<SocketRequestResult> RequestAsync<TRequest>(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId, selectionPolicy).RequestAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<TResponse> RequestAsync<TRequest, TResponse>(
      string nodeId,
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId).RequestAsync<TRequest, TResponse>(service, operation, payload, cancellationToken);
  }

  public async ValueTask<TResponse> RequestAsync<TRequest, TResponse>(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId, selectionPolicy).RequestAsync<TRequest, TResponse>(service, operation, payload, cancellationToken);
  }

  public async ValueTask SendEventAsync<TPayload>(
      string nodeId,
      string service,
      string operation,
      TPayload payload,
      CancellationToken cancellationToken = default)
  {
    await Get(nodeId).SendEventAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask SendEventAsync<TPayload>(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      string service,
      string operation,
      TPayload payload,
      CancellationToken cancellationToken = default)
  {
    await Get(nodeId, selectionPolicy).SendEventAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<Stream> OpenStreamAsync<TRequest>(
      string nodeId,
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId).OpenStreamAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<Stream> OpenStreamAsync<TRequest>(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      string service,
      string operation,
      TRequest payload,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId, selectionPolicy).OpenStreamAsync(service, operation, payload, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> RequestBatchAsync(
      string nodeId,
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId).RequestBatchAsync(items, timeout, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> RequestBatchAsync(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId, selectionPolicy).RequestBatchAsync(items, timeout, cancellationToken);
  }

  public async ValueTask SendEventBatchAsync(
      string nodeId,
      IEnumerable<SocketBatchItem> items,
      CancellationToken cancellationToken = default)
  {
    await Get(nodeId).SendEventBatchAsync(items, cancellationToken);
  }

  public async ValueTask SendEventBatchAsync(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      IEnumerable<SocketBatchItem> items,
      CancellationToken cancellationToken = default)
  {
    await Get(nodeId, selectionPolicy).SendEventBatchAsync(items, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> SendEventBatchWithResultsAsync(
      string nodeId,
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId).SendEventBatchWithResultsAsync(items, timeout, cancellationToken);
  }

  public async ValueTask<IReadOnlyList<SocketBatchResult>> SendEventBatchWithResultsAsync(
      string nodeId,
      SocketPeerSelectionPolicy selectionPolicy,
      IEnumerable<SocketBatchItem> items,
      TimeSpan timeout,
      CancellationToken cancellationToken = default)
  {
    return await Get(nodeId, selectionPolicy).SendEventBatchWithResultsAsync(items, timeout, cancellationToken);
  }

  public async ValueTask BroadcastEventAsync<TPayload>(
      string service,
      string operation,
      TPayload payload,
      CancellationToken cancellationToken = default)
  {
    foreach (var peer in Snapshot())
      await peer.SendEventAsync(service, operation, payload, cancellationToken);
  }

  public void PeerConnected(SocketPeer peer)
  {
    if (peer is null || string.IsNullOrWhiteSpace(peer.RemoteNodeId))
      return;

    while (true)
    {
      var nodePeers = Peers.GetOrAdd(peer.RemoteNodeId, _ => new NodePeerSet());
      if (nodePeers.TryAdd(peer))
        return;

      RemoveExact(peer.RemoteNodeId, nodePeers);
    }
  }

  public void PeerDisconnected(SocketPeer peer, Exception exception)
  {
    if (peer is null || string.IsNullOrWhiteSpace(peer.RemoteNodeId))
      return;

    if (!Peers.TryGetValue(peer.RemoteNodeId, out var nodePeers))
      return;

    if (!nodePeers.TryRemove(peer))
      return;

    if (nodePeers.TryRetireIfEmpty())
      RemoveExact(peer.RemoteNodeId, nodePeers);
  }

  void RemoveExact(string nodeId, NodePeerSet nodePeers)
  {
    ((ICollection<KeyValuePair<string, NodePeerSet>>)Peers)
        .Remove(new KeyValuePair<string, NodePeerSet>(nodeId, nodePeers));
  }

  sealed class NodePeerSet
  {
    readonly Lock Sync = new();
    readonly Dictionary<string, SocketPeer> Peers = new(StringComparer.Ordinal);
    SocketPeer[] PeersSnapshot = [];
    int CountValue;
    int RoundRobinIndex;
    bool Retired;

    public int Count => Volatile.Read(ref CountValue);

    public SocketPeer[] Snapshot()
    {
      return Volatile.Read(ref PeersSnapshot);
    }

    public SocketPeer[] PublicSnapshot()
    {
      var snapshot = Snapshot();
      return snapshot.Length == 0 ? [] : (SocketPeer[])snapshot.Clone();
    }

    public bool TryAdd(SocketPeer peer)
    {
      lock (Sync)
      {
        if (Retired)
          return false;

        Peers[peer.ConnectionId] = peer;
        PublishSnapshot();
        return true;
      }
    }

    public bool TryRemove(SocketPeer peer)
    {
      lock (Sync)
      {
        if (!Peers.TryGetValue(peer.ConnectionId, out var current) || !ReferenceEquals(current, peer))
          return false;

        Peers.Remove(peer.ConnectionId);
        PublishSnapshot();
        return true;
      }
    }

    public bool TryRetireIfEmpty()
    {
      lock (Sync)
      {
        if (Peers.Count != 0)
          return false;

        Retired = true;
        return true;
      }
    }

    public bool TrySelect(SocketPeerSelectionPolicy selectionPolicy, out SocketPeer peer)
    {
      var snapshot = Snapshot();
      if (snapshot.Length == 0)
      {
        peer = null;
        return false;
      }

      peer = selectionPolicy switch
      {
        SocketPeerSelectionPolicy.First => snapshot[0],
        SocketPeerSelectionPolicy.Random => snapshot[Random.Shared.Next(snapshot.Length)],
        _ => snapshot[NextRoundRobinIndex(snapshot.Length)]
      };
      return true;
    }

    void PublishSnapshot()
    {
      var snapshot = new SocketPeer[Peers.Count];
      Peers.Values.CopyTo(snapshot, 0);
      Volatile.Write(ref PeersSnapshot, snapshot);
      Volatile.Write(ref CountValue, snapshot.Length);
    }

    int NextRoundRobinIndex(int count)
    {
      var next = Interlocked.Increment(ref RoundRobinIndex) - 1;
      return (int)((uint)next % (uint)count);
    }
  }
}
