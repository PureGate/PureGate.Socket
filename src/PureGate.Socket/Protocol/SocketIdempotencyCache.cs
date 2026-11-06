using System.Collections.Concurrent;

namespace PureGate.Socket.Protocol;

sealed class SocketIdempotencyCache
{
  readonly ConcurrentDictionary<SocketIdempotencyKey, SocketIdempotencyEntry> Entries = new();
  readonly ConcurrentQueue<SocketIdempotencyQueueItem> InsertionOrder = new();

  public int Count => Entries.Count;

  public SocketIdempotencyEntry GetOrAdd(SocketIdempotencyKey key, SocketIdempotencyEntry entry)
  {
    var result = Entries.GetOrAdd(key, entry);
    if (ReferenceEquals(result, entry))
      InsertionOrder.Enqueue(new SocketIdempotencyQueueItem(key, entry));

    return result;
  }

  public void Remove(SocketIdempotencyKey key)
  {
    Entries.TryRemove(key, out _);
  }

  // Removes the key only while it still maps to this entry, so a newer attempt is never evicted.
  public void Remove(SocketIdempotencyKey key, SocketIdempotencyEntry entry)
  {
    Entries.TryRemove(new KeyValuePair<SocketIdempotencyKey, SocketIdempotencyEntry>(key, entry));
  }

  public bool Contains(SocketIdempotencyKey key)
  {
    return Entries.ContainsKey(key);
  }

  public void Prune(TimeSpan ttl, int maxEntries)
  {
    var now = DateTimeOffset.UtcNow;
    while (InsertionOrder.TryPeek(out var item))
    {
      if (!Entries.TryGetValue(item.Key, out var current) || !ReferenceEquals(current, item.Entry))
      {
        InsertionOrder.TryDequeue(out _);
        continue;
      }

      if (Entries.Count <= maxEntries && now - item.Entry.CreatedAt <= ttl)
        break;

      if (InsertionOrder.TryDequeue(out _))
        Entries.TryRemove(item.Key, out _);
    }
  }
}

readonly record struct SocketIdempotencyQueueItem(
    SocketIdempotencyKey Key,
    SocketIdempotencyEntry Entry);

readonly record struct SocketIdempotencyKey(
    string NodeId,
    string Service,
    string Operation,
    string IdempotencyKey);

sealed class SocketIdempotencyEntry
{
  readonly TaskCompletionSource<SocketResponse> Source = new(TaskCreationOptions.RunContinuationsAsynchronously);

  public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
  public Task<SocketResponse> Task => Source.Task;

  public void TrySetResult(SocketResponse response)
  {
    Source.TrySetResult(response);
  }

  public void TrySetCanceled()
  {
    Source.TrySetCanceled();
  }
}
