using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
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
