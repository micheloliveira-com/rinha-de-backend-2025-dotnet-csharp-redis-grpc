
using System.Collections.Concurrent;
using Grpc.Core;
using ReactiveLock.Distributed.Grpc;

public class LockGroup
{
    public ConcurrentDictionary<string, InstanceLockStatus> InstanceStates { get; } = new();
    public ConcurrentBag<IServerStreamWriter<LockStatusNotification>> Subscribers { get; } = new();
}