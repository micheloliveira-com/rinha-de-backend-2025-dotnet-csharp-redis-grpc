using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;

public class BusyMonitor
{
    private readonly IDatabase _redisDb;
    private readonly string _hashKey;

    public BusyMonitor(IConnectionMultiplexer redis, string serviceName = "http")
    {
        _redisDb = redis.GetDatabase();
        _hashKey = $"busy:{serviceName}";
    }

    public async Task<bool> AreAllIdleAsync()
    {
        var values = await _redisDb.HashGetAllAsync(_hashKey).ConfigureAwait(false);
        if (values.Length == 0) return true;

        foreach (var entry in values)
        {
            if (int.TryParse(entry.Value.ToString(), out var count) && count > 0)
                return false;
        }

        return true;
    }
}
