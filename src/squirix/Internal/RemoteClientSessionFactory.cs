using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core.Interceptors;
using Squirix.Internal.Cluster.Bootstrap;
using Squirix.Internal.Cluster.Membership;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Serialization;

namespace Squirix.Internal;

internal static class RemoteClientSessionFactory
{
    public static async ValueTask<IRemoteClientSession> ConnectAsync(SquirixOptions options, HttpMessageHandler? handler, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        var endpoints = NormalizeEndpoints(options.Endpoints);

        var peers = new Peer[endpoints.Length];
        for (var i = 0; i < endpoints.Length; i++)
        {
            peers[i] = new Peer
            {
                NodeId = string.Concat("endpoint-", i.ToString(CultureInfo.InvariantCulture)),
                Url = endpoints[i],
            };
        }

        var interceptor = BuildInterceptorChain(options);

        ClientPool? clients = null;
        try
        {
#pragma warning disable CA2000
            clients = new ClientPool(peers, static nodeId => new CallPolicy(peer: nodeId), handler, interceptor);
#pragma warning restore CA2000
            var primaryNodeId = await clients.WarmUpAsync(cancellationToken).ConfigureAwait(false);
            var bootstrapNodeIds = new string[clients.BootstrapNodeIds.Count];
            for (var i = 0; i < clients.BootstrapNodeIds.Count; i++)
                bootstrapNodeIds[i] = clients.BootstrapNodeIds[i];

            var failover = new BootstrapEndpointFailover(bootstrapNodeIds, primaryNodeId);
            var connected = clients;
            clients = null;
            return new RemoteClientSession(connected, failover, SerializationProvider.Create(options.Serializer));
        }
        finally
        {
            if (clients is not null)
                await clients.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static Interceptor? BuildInterceptorChain(SquirixOptions options)
    {
        var interceptors = new List<Interceptor>();

        if (!string.IsNullOrWhiteSpace(options.ApiKey))
            interceptors.Add(new ApiKeyInterceptor(options.ApiKey));

        if (options.BearerTokenProvider is not null)
            interceptors.Add(new BearerTokenInterceptor(options.BearerTokenProvider));

        return interceptors.Count switch
        {
            0 => null,
            1 => interceptors[0],
            _ => new CompositeInterceptor(interceptors),
        };
    }

    private static string[] NormalizeEndpoints(IEnumerable<string> endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();

        foreach (var endpoint in endpoints)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || string.IsNullOrWhiteSpace(uri.Scheme) || string.IsNullOrWhiteSpace(uri.Host))
                throw new ArgumentException($"Endpoint '{endpoint}' must be an absolute Squirix server URL.", nameof(endpoints));

            GrpcTransportEndpoints.RequireHttps(uri.AbsoluteUri);
            var normalized = uri.ToString();

            if (seen.Add(normalized))
                result.Add(normalized);
        }

        return result.Count == 0 ? throw new InvalidOperationException("At least one Squirix server endpoint must be configured.") : [.. result];
    }
}
