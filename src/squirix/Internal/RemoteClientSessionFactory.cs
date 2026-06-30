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
                Uri = endpoints[i],
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
            var failover = new BootstrapEndpointFailover(pool.BootstrapNodeIds, primaryNodeId);
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
        if (options.BearerTokenProvider is not { } tokenProvider)
            return null;

        return new BearerTokenCallCredentials(tokenProvider).Credentials;
    }

    private static Uri[] NormalizeEndpoints(IList<Uri> endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<Uri>();

        for (var index = 0; index < endpoints.Count; index++)
        {
            var endpoint = endpoints[index] ?? throw new ArgumentException("Endpoint must be a non-null absolute URI.", nameof(endpoints));
            if (!endpoint.IsAbsoluteUri || string.IsNullOrWhiteSpace(endpoint.Scheme) || string.IsNullOrWhiteSpace(endpoint.Host))
                throw new ArgumentException($"Endpoint '{endpoint}' must be an absolute Squirix server URL.", nameof(endpoints));

            GrpcTransportEndpoints.RequireHttps(endpoint);
            var authority = endpoint.GetLeftPart(UriPartial.Authority);
            if (seen.Add(authority))
                result.Add(endpoint);
        }

        return result.Count is 0 ? throw new InvalidOperationException("At least one Squirix server endpoint must be configured.") : [.. result];
    }

    private sealed class BearerTokenCallCredentials
    {
        private const string AuthorizationHeader = "authorization";
        private const string BearerSchemePrefix = "Bearer ";

        private readonly Func<CancellationToken, ValueTask<string>> _tokenProvider;

        internal BearerTokenCallCredentials(Func<CancellationToken, ValueTask<string>> tokenProvider)
        {
            _tokenProvider = tokenProvider;
            Credentials = CallCredentials.FromInterceptor(InterceptAsync);
        }

        internal CallCredentials Credentials { get; }

        private static async Task AddAuthorizationHeaderAsync(ValueTask<string> tokenTask, Metadata metadata)
        {
            var token = await tokenTask.ConfigureAwait(false);
            metadata.Add(AuthorizationHeader, string.Concat(BearerSchemePrefix, token));
        }

        private Task InterceptAsync(AuthInterceptorContext context, Metadata metadata) => AddAuthorizationHeaderAsync(_tokenProvider(context.CancellationToken), metadata);
    }
}
