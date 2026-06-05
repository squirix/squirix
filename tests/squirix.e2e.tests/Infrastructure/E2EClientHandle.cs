using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Represents a connected external SDK client.
/// </summary>
internal sealed class E2EClientHandle : IAsyncDisposable
{
    private readonly ISquirixClient _client;

    public E2EClientHandle(ISquirixClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    public ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken) => _client.GetCacheAsync<T>(cacheName, cancellationToken);

    public ValueTask DisposeAsync() => _client.DisposeAsync();
}
