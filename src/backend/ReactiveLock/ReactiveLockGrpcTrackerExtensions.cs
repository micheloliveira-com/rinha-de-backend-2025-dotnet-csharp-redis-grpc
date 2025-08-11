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
        LocalClient = new ReactiveLockGrpcClient(GrpcChannel.ForAddress(localGrpc));
        RemoteClients.AddRange(remotes.Select(url => new ReactiveLockGrpcClient(GrpcChannel.ForAddress(url))));
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
