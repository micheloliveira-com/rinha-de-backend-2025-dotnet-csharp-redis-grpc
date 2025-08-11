using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Replication.Grpc;

public class PaymentReplicationClientManager
{
    private readonly List<PaymentReplication.PaymentReplicationClient> _remoteClients = new();

    public PaymentReplicationClientManager(string local, params string[] remoteGrpcUrls)
    {
        foreach (var url in remoteGrpcUrls)
        {
            if (local == url)
            {
                break;
            }
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
