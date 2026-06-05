using System;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Bootstrap;
using Squirix.Internal.Cluster.Transport;
using Squirix.Serialization;

namespace Squirix.Internal;

internal sealed class RemoteClientSession : IRemoteClientSession
{
    private readonly BootstrapEndpointFailover _bootstrapFailover;
    private readonly IClientPool _remoteClients;
    private readonly ISquirixSerializer _serializer;

    public RemoteClientSession(IClientPool remoteClients, BootstrapEndpointFailover bootstrapFailover, ISquirixSerializer serializer)
    {
        _remoteClients = remoteClients ?? throw new ArgumentNullException(nameof(remoteClients));
        _bootstrapFailover = bootstrapFailover ?? throw new ArgumentNullException(nameof(bootstrapFailover));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    public async ValueTask DisposeAsync()
    {
        _remoteClients.BeginDrain();
        await _remoteClients.DisposeAsync().ConfigureAwait(false);
    }

    public ICache<T> GetCache<T>(string cacheName) => new RemoteCache<T>(cacheName, _bootstrapFailover, _remoteClients, _serializer);
}
