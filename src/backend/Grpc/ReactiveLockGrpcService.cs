using System.Collections.Concurrent;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MichelOliveira.Com.ReactiveLock.Core;
using MichelOliveira.Com.ReactiveLock.DependencyInjection;
using System.Linq;
using System.Threading.Tasks;
using System;
using Google.Protobuf.WellKnownTypes;
using ReactiveLock.Distributed.Grpc;


public class ReactiveLockGrpcService : ReactiveLockGrpc.ReactiveLockGrpcBase
{
    private class LockGroup
    {
        public ConcurrentDictionary<string, InstanceLockStatus> InstanceStates { get; } = new();
        public ConcurrentBag<IServerStreamWriter<LockStatusNotification>> Subscribers { get; } = new();
    }

    private readonly ConcurrentDictionary<string, LockGroup> _groups = new();

    public override async Task<Empty> SetStatus(LockStatusRequest request, ServerCallContext context)
    {
        var group = _groups.GetOrAdd(request.LockKey, _ => new LockGroup());
        group.InstanceStates[request.InstanceId] =
                new InstanceLockStatus()
                {
                    IsBusy = request.IsBusy,
                    LockData = request.LockData
                };
        await BroadcastAsync(request.LockKey, group);
        return new Empty();
    }

    public override async Task SubscribeLockStatus(IAsyncStreamReader<LockStatusRequest> requestStream,
                                                   IServerStreamWriter<LockStatusNotification> responseStream,
                                                   ServerCallContext context)
    {
        await foreach (var req in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
        {
            var group = _groups.GetOrAdd(req.LockKey, _ => new LockGroup());
            group.Subscribers.Add(responseStream);

            await responseStream.WriteAsync(new LockStatusNotification
            {
                LockKey = req.LockKey,
                InstancesStatus = { group.InstanceStates }
            }).ConfigureAwait(false);

            break;
        }

        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken).ConfigureAwait(false);
        }
        catch { }
    }

    private async Task BroadcastAsync(string lockKey, LockGroup group)
    {
        var notification = new LockStatusNotification
        {
            LockKey = lockKey,
            InstancesStatus = { group.InstanceStates }
        };

        foreach (var subscriber in group.Subscribers.ToArray())
        {
            try
            {
                await subscriber.WriteAsync(notification).ConfigureAwait(false);
            }
            catch
            {
                group.Subscribers.TryTake(out _);
            }
        }
    }
}
