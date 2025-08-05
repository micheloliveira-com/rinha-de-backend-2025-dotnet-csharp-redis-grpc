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

public static class ReactiveLockGrpcTrackerExtensions
{
    private static readonly ConcurrentQueue<string> RegisteredLocks = new();
    private static string? StoredInstanceName;
    private static ReactiveLockGrpcClient? LocalClient;
    private static readonly List<ReactiveLockGrpcClient> RemoteClients = new();

    public static void InitializeDistributedGrpcReactiveLock(this IServiceCollection services, string instanceName, string localGrpc, params string[] remotes)
    {
        //Console.WriteLine($"[Init] Initializing ReactiveLock. Instance={instanceName}, Local={localGrpc}");

        ReactiveLockConventions.RegisterFactory(services);
        StoredInstanceName = instanceName;
                    var httpHandler = new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
                MaxConnectionsPerServer = int.MaxValue,
                EnableMultipleHttp2Connections = true
            };
        LocalClient = new ReactiveLockGrpcClient(GrpcChannel.ForAddress(localGrpc, new GrpcChannelOptions
            {
                HttpHandler = httpHandler
            }));
        RemoteClients.AddRange(remotes.Select(url => new ReactiveLockGrpcClient(GrpcChannel.ForAddress(url, new GrpcChannelOptions
            {
                HttpHandler = httpHandler
            }))));
    }

    public static IServiceCollection AddDistributedGrpcReactiveLock(
        this IServiceCollection services,
        string lockKey,
        IEnumerable<Func<IServiceProvider, Task>>? onLockedHandlers = null,
        IEnumerable<Func<IServiceProvider, Task>>? onUnlockedHandlers = null)
    {
        if (LocalClient is null || string.IsNullOrEmpty(StoredInstanceName))
            throw new InvalidOperationException("You must call InitializeDistributedGrpcReactiveLock first.");

        ReactiveLockConventions.RegisterState(services, lockKey, onLockedHandlers, onUnlockedHandlers);
        ReactiveLockConventions.RegisterController(services, lockKey, _ =>
        {
            var store = new ReactiveLockGrpcTrackerStore(LocalClient, lockKey, StoredInstanceName);
            return new ReactiveLockTrackerController(store, StoredInstanceName);
        });

        RegisteredLocks.Enqueue(lockKey);
        return services;
    }

    public static async Task UseDistributedGrpcReactiveLockAsync(this IApplicationBuilder app)
    {
        var factory = app.ApplicationServices.GetRequiredService<IReactiveLockTrackerFactory>();

        foreach (var lockKey in RegisteredLocks)
        {
            var state = factory.GetTrackerState(lockKey);
            var controller = factory.GetTrackerController(lockKey);
            await controller.DecrementAsync().ConfigureAwait(false);

            async Task SubscribeToUpdates(ReactiveLockGrpcClient client, string source)
            {
                try
                {
                    var call = client.SubscribeLockStatus();
                    await call.RequestStream.WriteAsync(new LockStatusRequest
                    {
                        LockKey = lockKey,
                        InstanceId = StoredInstanceName!
                    }).ConfigureAwait(false);

                    await foreach (var update in call.ResponseStream.ReadAllAsync().ConfigureAwait(false))
                    {
                        //Console.WriteLine($"[{source}] Update for {lockKey}: AllIdle={update.InstancesStatus.All(x => !x.Value)}");

                        if (update.InstancesStatus.All(x => !x.Value))
                            await state.SetLocalStateUnblockedAsync().ConfigureAwait(false);
                        else
                            await state.SetLocalStateBlockedAsync().ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    //Console.WriteLine($"[{source}] Subscription failed for {lockKey}: {ex.Message}");
                }
            }

            _ = Task.Run(() => SubscribeToUpdates(LocalClient!, "Local"));
            foreach (var remote in RemoteClients)
                _ = Task.Run(() => SubscribeToUpdates(remote, "Remote"));
        }

        // Reset state
        //StoredInstanceName = null;
        //LocalClient = null;
        //RemoteClients.Clear();
    }
}

public class ReactiveLockGrpcService : ReactiveLockGrpc.ReactiveLockGrpcBase
{
    private class LockGroup
    {
        public ConcurrentDictionary<string, bool> InstanceStates { get; } = new();
        public ConcurrentBag<IServerStreamWriter<LockStatusNotification>> Subscribers { get; } = new();
    }

    private readonly ConcurrentDictionary<string, LockGroup> _groups = new();

    public override async Task<Empty> SetStatus(LockStatusRequest request, ServerCallContext context)
    {
        var group = _groups.GetOrAdd(request.LockKey, _ => new LockGroup());
        group.InstanceStates[request.InstanceId] = request.IsBusy;
        //_ = BroadcastAsync(request.LockKey, group);
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

            // Send initial state
            await responseStream.WriteAsync(new LockStatusNotification
            {
                LockKey = req.LockKey,
                InstancesStatus = { group.InstanceStates }
            }).ConfigureAwait(false);

            break; // Process only the first message
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
