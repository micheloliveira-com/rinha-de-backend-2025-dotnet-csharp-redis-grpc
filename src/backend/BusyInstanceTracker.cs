using System.Net;
using StackExchange.Redis;

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
            // Set this instance to busy
            await _redisDb.HashSetAsync(_hashKey, _hostname, "1").ConfigureAwait(false);
            await _subscriber.PublishAsync(RedisChannel.Literal(_channelName), "true").ConfigureAwait(false);
        }
    }

    public async Task DecrementAsync()
    {
        var afterCount = Interlocked.Decrement(ref _inFlightRequestCount);
        if (afterCount == 0)
        {
            // Set this instance to idle (or optionally remove the field)
            await _redisDb.HashSetAsync(_hashKey, _hostname, "0").ConfigureAwait(false);
            await _subscriber.PublishAsync(RedisChannel.Literal(_channelName), "false").ConfigureAwait(false);
        }
    }
}
