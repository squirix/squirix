using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;

namespace Squirix.Benchmarks.Support.Client;

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

    internal ISquirixClient Client => BenchmarkThrowHelper.Disposed(_client, nameof(BenchmarkClientLease));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        var client = _client;
        _client = null;
        if (client != null)
            await client.DisposeAsync().ConfigureAwait(false);
    }

    internal static async Task<BenchmarkClientLease> ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var client = await SquirixClient.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
        return new BenchmarkClientLease(client);
    }
}
