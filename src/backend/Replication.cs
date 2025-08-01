using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using ReactiveLock.Grpc;
using Replication.Grpc;

public class PaymentReplicationService : PaymentReplication.PaymentReplicationBase
{
    private readonly ConcurrentBag<PaymentInsertRpcParameters> _replicatedPayments = new();

    public override async Task<Google.Protobuf.WellKnownTypes.Empty> PublishPayments(
        IAsyncStreamReader<PaymentInsertRpcParameters> requestStream,
        ServerCallContext context)
    {
        await foreach (var payment in requestStream.ReadAllAsync(context.CancellationToken).ConfigureAwait(false))
        {
            //Console.WriteLine($"[RECEIVED] Payment {payment.CorrelationId} from {payment.SourceInstance}");
            HandleLocally(payment);
        }

        return new Google.Protobuf.WellKnownTypes.Empty();
    }

    public void HandleLocally(PaymentInsertRpcParameters payment)
    {
        _replicatedPayments.Add(payment);
    }

    public PaymentInsertRpcParameters[] GetReplicatedPaymentsSnapshot()
    {
        return _replicatedPayments.ToArray();
    }
}

public class PaymentReplicationClientManager
{
    private readonly List<PaymentReplication.PaymentReplicationClient> _remoteClients = new();

    public PaymentReplicationClientManager(params string[] remoteGrpcUrls)
    {
        foreach (var url in remoteGrpcUrls)
        {
            var channel = GrpcChannel.ForAddress(url);
            _remoteClients.Add(new PaymentReplication.PaymentReplicationClient(channel));
        }
    }

    public async Task PublishPaymentsBatchAsync(IEnumerable<PaymentInsertRpcParameters> payments, PaymentReplicationService paymentReplicationService)
    {
        foreach (var pay in payments)
        {
            paymentReplicationService.HandleLocally(pay);
        }
        // Send batch to each remote client
        foreach (var remoteClient in _remoteClients)
        {
            using (var remoteCall = remoteClient.PublishPayments())
            {
                foreach (var payment in payments)
                {
                    await remoteCall.RequestStream.WriteAsync(payment).ConfigureAwait(false);
                }

                await remoteCall.RequestStream.CompleteAsync().ConfigureAwait(false);
                await remoteCall.ResponseAsync.ConfigureAwait(false);
            }
        }
    }

}
