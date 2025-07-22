using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Net;

public class CountingHandler : DelegatingHandler
{
    private readonly BusyInstanceTracker _tracker;

    public CountingHandler(BusyInstanceTracker tracker)
    {
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        await _tracker.IncrementAsync();

        try
        {
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await _tracker.DecrementAsync();
        }
    }
}
