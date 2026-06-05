using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Owns one connected <see cref="ISquirixClient" /> and disposes it exactly once.
/// </summary>
internal sealed class BenchmarkClientLease : IAsyncDisposable
{
    private ISquirixClient? _client;
    private int _disposed;

    private BenchmarkClientLease(ISquirixClient client)
    {
        _client = client;
    }

    internal ISquirixClient Client => _client ?? throw new ObjectDisposedException(nameof(BenchmarkClientLease));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        var client = _client;
        _client = null;
        if (client is not null)
            await client.DisposeAsync().ConfigureAwait(false);
    }

    internal static async Task<BenchmarkClientLease> ConnectAsync(string endpoint, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        var client = await SquirixClient.ConnectAsync(
            options =>
            {
                BenchmarkRuntime.ConfigureRemoteClient(options);
                options.Endpoints.Add(endpoint);
            },
            cancellationToken).ConfigureAwait(false);
        return new BenchmarkClientLease(client);
    }
}
