using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using Replication.Grpc;

public class GrpcQueueWorker : BackgroundService
{
    private readonly PaymentReplicationService _replicationService;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly int _workerCount;

    public GrpcQueueWorker(PaymentReplicationService replicationService, IServiceScopeFactory scopeFactory, IOptions<DefaultOptions> options)
    {
        _replicationService = replicationService;
        _scopeFactory = scopeFactory;
        _workerCount = options.Value.WORKER_SIZE;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var workers = new Task[_workerCount];
        for (int i = 0; i < _workerCount; i++)
        {
            workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);
        }
        return Task.WhenAll(workers);
    }

    private async Task WorkerLoopAsync(CancellationToken stoppingToken)
    {
        var queue = _replicationService.GetQueue();

        using var scope = _scopeFactory.CreateScope();
        var paymentService = scope.ServiceProvider.GetRequiredService<PaymentService>();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (queue.TryDequeue(out var rawPayload))
                {
                    await paymentService.ProcessPaymentAsync(rawPayload).ConfigureAwait(false);
                }
                else
                {
                    // Queue empty, delay to avoid busy loop
                    await Task.Delay(10, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // graceful shutdown
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GrpcQueueWorker] Worker error: {ex}");
            }
        }
    }
}
