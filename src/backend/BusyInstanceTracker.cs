using System.Net;
using StackExchange.Redis;
using System.Threading;

public class BusyInstanceTracker
{
    private readonly IDatabase _redisDb;
    private readonly ISubscriber _subscriber;
    private readonly string _serviceName;
    private readonly string _hostname;
    private readonly string _hashKey;
    private readonly string _channelName;

    private int _inFlightRequestCount;

    public BusyInstanceTracker(IConnectionMultiplexer redis, string serviceName = "http")
    {
        _redisDb = redis.GetDatabase();
        _subscriber = redis.GetSubscriber();
        _serviceName = serviceName;
        _hostname = Dns.GetHostName();
        _hashKey = $"busy:{_serviceName}";
        _channelName = $"channel:busy:{_serviceName}";
    }

    public async Task IncrementAsync()
    {
        var newCount = Interlocked.Increment(ref _inFlightRequestCount);
        if (newCount == 1)
        {
            await _redisDb.HashSetAsync(_hashKey, _hostname, "1").ConfigureAwait(false);
            await _subscriber.PublishAsync(RedisChannel.Literal(_channelName), "true").ConfigureAwait(false);
        }
    }

    public async Task DecrementAsync(int amount = 1)
    {
        var afterCount = Interlocked.Add(ref _inFlightRequestCount, -amount);
        if (afterCount <= 0)
        {
            Interlocked.Exchange(ref _inFlightRequestCount, 0);
            await _redisDb.HashSetAsync(_hashKey, _hostname, "0").ConfigureAwait(false);
            await _subscriber.PublishAsync(RedisChannel.Literal(_channelName), "false").ConfigureAwait(false);
        }
    }
}
