using Dapper;
using MichelOliveira.Com.ReactiveLock.Core;
using MichelOliveira.Com.ReactiveLock.DependencyInjection;
using MichelOliveira.Com.ReactiveLock.Distributed.Redis;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Polly;
using Polly.Extensions.Http;
using Polly.Retry;
using StackExchange.Redis;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
[module: DapperAot]

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = builder.Configuration.GetConnectionString("redis")!;
    var options = ConfigurationOptions.Parse(configuration);

    var retryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetry(
            retryCount: 60,
            sleepDurationProvider: _ => TimeSpan.FromSeconds(1),
            onRetry: (exception, timeSpan, retryCount, context) =>
            {
                Console.WriteLine($"[Redis] Retry {retryCount}: {exception.GetType().Name} - {exception.Message}");
            });

    return retryPolicy.Execute(() =>
    {
        Console.WriteLine("[Redis] Attempting connection...");
        var muxer = ConnectionMultiplexer.Connect(options);

        if (!muxer.IsConnected)
            throw new Exception("Redis connection failed (IsConnected = false)");

        Console.WriteLine("[Redis] Connected successfully.");
        return muxer;
    });
});

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, JsonContext.Default);
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
});

builder.Services.AddHttpClient(defaultProcessorName, o =>
    o.BaseAddress = new Uri(builder.Configuration.GetConnectionString(defaultProcessorName)!))
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        MaxConnectionsPerServer = int.MaxValue,
        PooledConnectionLifetime = TimeSpan.FromMinutes(10),
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
        EnableMultipleHttp2Connections = true,
        ConnectTimeout = TimeSpan.FromSeconds(5),
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
    })
    .AddHttpMessageHandler<CountingHandler>();

builder.Services.AddTransient<IDbConnection>(sp =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("postgres")));
builder.Services.AddTransient<CountingHandler>();
builder.Services.AddSingleton<PaymentService>();
builder.Services.AddSingleton<PaymentSummaryService>();
builder.Services.AddSingleton<PaymentBatchInserterService>();
builder.Services.AddSingleton<RedisQueueWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<RedisQueueWorker>());

if (builder.Environment.IsProduction())
{
    builder.Logging.ClearProviders();
    builder.Logging.SetMinimumLevel(LogLevel.Error);
}
builder.Services.InitializeDistributedRedisReactiveLock(Dns.GetHostName());

builder.Services.AddDistributedRedisReactiveLock(Constant.REACTIVELOCK_HTTP_NAME);
builder.Services.AddDistributedRedisReactiveLock(Constant.REACTIVELOCK_POSTGRES_NAME);
builder.Services.AddDistributedRedisReactiveLock(Constant.REACTIVELOCK_API_PAYMENTS_SUMMARY_NAME, [
    async(sp) => {
        var summary = sp.GetRequiredService<PaymentSummaryService>();
        await summary.FlushWhileGateBlockedAsync();
    }
]);

var app = builder.Build();

await app.UseDistributedRedisReactiveLockAsync();

var apiGroup = app.MapGroup("/");
apiGroup.MapGet("/", () => Results.Ok());

apiGroup.MapPost("payments", async (HttpContext context,
    [FromServices] PaymentService paymentService) =>
{
    return await paymentService.EnqueuePaymentAsync(context);
});

apiGroup.MapGet("/payments-summary", async (
    [FromQuery] DateTimeOffset? from,
    [FromQuery] DateTimeOffset? to,
    [FromServices] PaymentSummaryService paymentsSummaryService) =>
{
    return await paymentsSummaryService.GetPaymentsSummaryAsync(from, to);
});

apiGroup.MapPost("/purge-payments", async (
    [FromServices] PaymentService paymentService) =>
{
    return await paymentService.PurgePaymentsAsync();
});

app.Run();

