namespace PureGate.Socket.Protocol;

// The validated lanes of SocketFabricOptions.HandlerLanes, with a lookup from a request's service and
// operation to its lane. Built once per options; looking up allocates nothing.
sealed class SocketHandlerLaneMap
{
  readonly Dictionary<string, ServiceLanes> Services = new(StringComparer.Ordinal);

  SocketHandlerLaneMap(SocketHandlerLane[] lanes)
  {
    Lanes = lanes;
    for (var index = 0; index < lanes.Length; index++)
    {
      var lane = lanes[index];
      if (!Services.TryGetValue(lane.Service, out var service))
        Services.Add(lane.Service, service = new ServiceLanes());

      if (lane.Operation is null)
      {
        if (service.AllOperations >= 0)
          throw Duplicate(lane);
        service.AllOperations = index;
      }
      else
      {
        service.Operations ??= new Dictionary<string, int>(StringComparer.Ordinal);
        if (!service.Operations.TryAdd(lane.Operation, index))
          throw Duplicate(lane);
      }
    }
  }

  // The normalized lanes, indexed as Find returns them.
  public SocketHandlerLane[] Lanes { get; }

  // Null when there are no lanes. Throws ArgumentException for a lane without a service, with an empty
  // operation, or that repeats another; clamps each lane's MaxConcurrentHandlers like the general one.
  public static SocketHandlerLaneMap Create(IReadOnlyList<SocketHandlerLane> lanes)
  {
    if (lanes is null || lanes.Count == 0)
      return null;

    var normalized = new SocketHandlerLane[lanes.Count];
    for (var i = 0; i < lanes.Count; i++)
    {
      var lane = lanes[i] ?? throw new ArgumentException("A handler lane is null.", nameof(SocketFabricOptions.HandlerLanes));
      if (string.IsNullOrEmpty(lane.Service))
        throw new ArgumentException("A handler lane needs a service.", nameof(SocketFabricOptions.HandlerLanes));
      if (lane.Operation is { Length: 0 })
        throw new ArgumentException(
            $"Handler lane for service '{lane.Service}' has an empty operation; use null for every operation of the service.",
            nameof(SocketFabricOptions.HandlerLanes));
      normalized[i] = lane with { MaxConcurrentHandlers = Math.Clamp(lane.MaxConcurrentHandlers, 1, 1024) };
    }

    return new SocketHandlerLaneMap(normalized);
  }

  // The index of the lane that runs requests for this service and operation, or -1 for the general handlers.
  public int Find(string service, string operation)
  {
    if (service is null || !Services.TryGetValue(service, out var lanes))
      return -1;
    if (lanes.Operations is not null && operation is not null && lanes.Operations.TryGetValue(operation, out var index))
      return index;
    return lanes.AllOperations;
  }

  // Message credit a sender keeps for lane messages out of the remote's window: as many as the lanes run at
  // once, and at most half the window, so general messages always keep the other half. None when the window
  // is too small to split.
  public long CreditReserve(int remoteWindow)
  {
    if (remoteWindow < 2)
      return 0;

    long total = 0;
    foreach (var lane in Lanes)
      total += lane.MaxConcurrentHandlers;
    return Math.Clamp(total, 1, remoteWindow / 2);
  }

  static ArgumentException Duplicate(SocketHandlerLane lane)
  {
    var operation = lane.Operation is null ? "every operation" : $"operation '{lane.Operation}'";
    return new ArgumentException(
        $"Handler lanes list service '{lane.Service}', {operation}, more than once.",
        nameof(SocketFabricOptions.HandlerLanes));
  }

  sealed class ServiceLanes
  {
    public int AllOperations = -1;
    public Dictionary<string, int> Operations;
  }
}
