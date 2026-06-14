using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Internal.Cluster.Bootstrap;
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

        var callCredentials = BuildCallCredentials(options);

        ClientPool? clients = null;
        try
        {
#pragma warning disable CA2000
            clients = new ClientPool(peers, static nodeId => new CallPolicy(peer: nodeId), handler, callCredentials: callCredentials);
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

    private static CallCredentials? BuildCallCredentials(SquirixOptions options)
    {
        return options.BearerTokenProvider is null ? null : CallCredentials.FromInterceptor(async (context, metadata) =>
        {
            var token = await options.BearerTokenProvider(context.CancellationToken).ConfigureAwait(false);
            metadata.Add("authorization", $"Bearer {token}");
        });
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
