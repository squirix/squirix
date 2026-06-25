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
                NodeId = $"endpoint-{i.ToString(CultureInfo.InvariantCulture)}",
                Url = endpoints[i],
            };
        }

        var credentials = BuildCallCredentials(options);

        ClientPool? pool = null;
        try
        {
#pragma warning disable CA2000
            pool = new ClientPool(peers, CallPolicyDefaults.Create, handler, callCredentials: credentials);
#pragma warning restore CA2000
            var primaryNodeId = await pool.WarmUpAsync(cancellationToken).ConfigureAwait(false);
            var nodeIds = new string[pool.BootstrapNodeIds.Count];
            for (var i = 0; i < pool.BootstrapNodeIds.Count; i++)
                nodeIds[i] = pool.BootstrapNodeIds[i];

            var failover = new BootstrapEndpointFailover(nodeIds, primaryNodeId);
            var connected = pool;
            pool = null;
            return new RemoteClientSession(connected, failover, SerializationProvider.Create(options.Serializer));
        }
        finally
        {
            if (pool is not null)
                await pool.DisposeAsync().ConfigureAwait(false);
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
            if (string.IsNullOrWhiteSpace(endpoint))
                throw new ArgumentException("Endpoint must be a non-empty string.", nameof(endpoints));

            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || string.IsNullOrWhiteSpace(uri.Scheme) || string.IsNullOrWhiteSpace(uri.Host))
                throw new ArgumentException($"Endpoint '{endpoint}' must be an absolute Squirix server URL.", nameof(endpoints));

            GrpcTransportEndpoints.RequireHttps(uri.AbsoluteUri);
            var normalized = uri.ToString();

            if (seen.Add(normalized))
                result.Add(normalized);
        }

        return result.Count is 0 ? throw new InvalidOperationException("At least one Squirix server endpoint must be configured.") : [.. result];
    }
}
