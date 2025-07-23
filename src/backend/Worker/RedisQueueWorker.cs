using StackExchange.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

public class RedisQueueWorker : BackgroundService
{
    private readonly IDatabase _db;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly string _queueKey = "task-queue";
    private readonly int _parallelism = 10;

    public RedisQueueWorker(IConnectionMultiplexer redis, IServiceScopeFactory scopeFactory)
    {
        _db = redis.GetDatabase();
        _scopeFactory = scopeFactory;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var workers = new Task[_parallelism];
        for (int i = 0; i < _parallelism; i++)
        {
            workers[i] = Task.Run(() => WorkerLoopAsync(stoppingToken), stoppingToken);
        }

        return Task.WhenAll(workers);
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                RedisValue msg;

                while ((msg = await _db.ListLeftPopAsync(_queueKey).ConfigureAwait(false)).HasValue)
                {
                    using var scope = _scopeFactory.CreateScope();
                    var paymentService = scope.ServiceProvider.GetRequiredService<PaymentService>();
                    await paymentService.ProcessPaymentAsync(msg!).ConfigureAwait(false);
                }
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Worker Error] {ex}");
            }
        }
    }
}
