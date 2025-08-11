using System.Collections.Concurrent;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MichelOliveira.Com.ReactiveLock.Core;
using MichelOliveira.Com.ReactiveLock.DependencyInjection;
using ReactiveLock.Grpc;
using static ReactiveLock.Grpc.ReactiveLockGrpc;
using System.Linq;
using System.Threading.Tasks;
using System;
using Google.Protobuf.WellKnownTypes;

public class ReactiveLockGrpcTrackerStore(ReactiveLockGrpcClient client, string lockKey, string instanceName)
    : IReactiveLockTrackerStore
{
    public static (bool allIdle, string? lockData) AreAllIdleFromRpc(LockStatusNotification update)
    {
        if (update.InstancesStatus.Count == 0)
            return (true, null);

        var busyEntries = update.InstancesStatus
            .Where(kv => kv.Value.IsBusy)
            .Select(kv => (busyPart: "1", extraPart: kv.Value.LockData))
            .ToArray();

        if (busyEntries.Length == 0)
            return (true, null);

        var extraParts = busyEntries
            .Select(x => x.extraPart)
            .Where(extra => !string.IsNullOrEmpty(extra))
            .ToArray();

        string? lockData = extraParts.Length > 0
            ? string.Join(IReactiveLockTrackerState.LOCK_DATA_SEPARATOR, extraParts)
            : null;

        return (false, lockData);
    }

    public async Task SetStatusAsync(string _, bool isBusy, string? lockData = null)
    {
        await client.SetStatusAsync(new LockStatusRequest
        {
            LockKey = lockKey,
            InstanceId = instanceName,
            IsBusy = isBusy,
            LockData = lockData
        }).ConfigureAwait(false);
    }
}