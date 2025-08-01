using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using ReactiveLock.Grpc;
using Replication.Grpc;

public class PaymentReplicationService : PaymentReplication.PaymentReplicationBase
{
    private readonly ConcurrentBag<IServerStreamWriter<PaymentInsertRpcParameters>> _subscribers = new();

    private readonly ConcurrentBag<PaymentInsertRpcParameters> _replicatedPayments = new();


    public override async Task<Google.Protobuf.WellKnownTypes.Empty> PublishPayments(IAsyncStreamReader<PaymentInsertRpcParameters> requestStream,
                                                      ServerCallContext context)
    {
        await foreach (var payment in requestStream.ReadAllAsync(context.CancellationToken))
        {
            //Console.WriteLine($"[RECEIVED] Payment {payment.CorrelationId} from {payment.SourceInstance}");

            HandleLocally(payment);
            await BroadcastAsync(payment);
        }

        return await Task.FromResult(new Google.Protobuf.WellKnownTypes.Empty());
    }
    private void HandleLocally(PaymentInsertRpcParameters payment)
    {
        _replicatedPayments.Add(payment);
    }

    private async Task BroadcastAsync(PaymentInsertRpcParameters payment)
    {
        foreach (var subscriber in _subscribers.ToArray())
        {
            try
            {
                await subscriber.WriteAsync(payment);
            }
            catch
            {
                _subscribers.TryTake(out _);
            }
        }
    }

    public PaymentInsertRpcParameters[] GetReplicatedPaymentsSnapshot()
    {
        return _replicatedPayments.ToArray();
    }
}

public class PaymentReplicationClientManager
{
    private readonly PaymentReplication.PaymentReplicationClient _localClient;
    private readonly List<PaymentReplication.PaymentReplicationClient> _remoteClients = new();

    private readonly ConcurrentBag<PaymentInsertRpcParameters> _localReplicatedPayments = new();

    public PaymentReplicationClientManager(string localGrpcUrl, params string[] remoteGrpcUrls)
    {
        var localChannel = GrpcChannel.ForAddress(localGrpcUrl);
        _localClient = new PaymentReplication.PaymentReplicationClient(localChannel);

        foreach (var url in remoteGrpcUrls)
        {
            var channel = GrpcChannel.ForAddress(url);
            _remoteClients.Add(new PaymentReplication.PaymentReplicationClient(channel));
        }
    }

    public async Task PublishPaymentsBatchAsync(IEnumerable<PaymentInsertRpcParameters> payments)
    {
        // Send batch to local client
        using (var localCall = _localClient.PublishPayments())
        {
            foreach (var payment in payments)
            {
                await localCall.RequestStream.WriteAsync(payment);
            }

            await localCall.RequestStream.CompleteAsync();
            await localCall.ResponseAsync;
        }

        // Send batch to each remote client
        foreach (var remoteClient in _remoteClients)
        {
            using (var remoteCall = remoteClient.PublishPayments())
            {
                foreach (var payment in payments)
                {
                    await remoteCall.RequestStream.WriteAsync(payment);
                }

                await remoteCall.RequestStream.CompleteAsync();
                await remoteCall.ResponseAsync;
            }
        }
    }



    // Call this to publish a batch or individual payment locally and remotely
    public async Task PublishPaymentAsync(PaymentInsertRpcParameters payment)
    {
        // Publish to local server (can be optional if local server already receives)
        using var localCall = _localClient.PublishPayments();
        await localCall.RequestStream.WriteAsync(payment);
        await localCall.RequestStream.CompleteAsync();
        await localCall.ResponseAsync;

        // Publish to replicas
        foreach (var client in _remoteClients)
        {
            using var call = client.PublishPayments();
            await call.RequestStream.WriteAsync(payment);
            await call.RequestStream.CompleteAsync();
            await call.ResponseAsync;
        }
    }
}