using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Transport;

namespace Squirix.Internal;

[Immutable]
internal sealed class RemoteClientSession : IAsyncDisposable
{
    private readonly EndpointFailover _bootstrapFailover;
    private readonly IClientPool _remoteClients;
    private readonly ISquirixSerializer _serializer;

    internal RemoteClientSession(IClientPool remoteClients, EndpointFailover bootstrapFailover, ISquirixSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(remoteClients);
        ArgumentNullException.ThrowIfNull(bootstrapFailover);
        ArgumentNullException.ThrowIfNull(serializer);
        _remoteClients = remoteClients;
        _bootstrapFailover = bootstrapFailover;
        _serializer = serializer;
    }

    public ValueTask DisposeAsync()
    {
        _remoteClients.BeginDrain();
        return _remoteClients.DisposeAsync();
    }

    public ICache<T> GetCache<T>(string cacheName) => new RemoteCache<T>(cacheName, _bootstrapFailover, _remoteClients, _serializer);
}
