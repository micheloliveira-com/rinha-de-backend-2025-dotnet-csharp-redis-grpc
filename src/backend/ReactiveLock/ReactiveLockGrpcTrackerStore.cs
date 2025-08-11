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
    public async Task SetStatusAsync(string _, bool isBusy)
    {
        //Console.WriteLine($"[TrackerStore] SetStatusAsync LockKey={lockKey}, Instance={instanceName}, IsBusy={isBusy}");
        await client.SetStatusAsync(new LockStatusRequest
        {
            LockKey = lockKey,
            InstanceId = instanceName,
            IsBusy = isBusy
        }).ConfigureAwait(false);
    }
}