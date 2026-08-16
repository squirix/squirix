using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;

namespace Squirix.Internal;

internal static class RemoteClientSessionFactory
{
    internal static async ValueTask<IRemoteClientSession> ConnectAsync(
        IList<Uri> endpoints,
        Func<CancellationToken, ValueTask<string>>? bearerTokenProvider,
        ISquirixSerializer? serializer,
        HttpMessageHandler? handler,
        CancellationToken cancellationToken)
    {
        var normalizedEndpoints = NormalizeEndpoints(endpoints);

        var peers = new Peer[normalizedEndpoints.Length];
        for (var i = 0; i < normalizedEndpoints.Length; i++)
        {
            peers[i] = new Peer
            {
                NodeId = FormatEndpointNodeId(i),
                Uri = normalizedEndpoints[i],
            };
        }

        var credentials = BuildCallCredentials(bearerTokenProvider);

        ClientPool? pool = null;
        try
        {
#pragma warning disable CA2000
            pool = new ClientPool(peers, CallPolicyDefaults.Create, handler, callCredentials: credentials);
#pragma warning restore CA2000
            var primaryNodeId = await pool.WarmUpAsync(cancellationToken).ConfigureAwait(false);
            var failover = new EndpointFailover(pool.BootstrapNodeIds, primaryNodeId);
            var connected = pool;
            pool = null;
            return new RemoteClientSession(connected, failover, SerializationProvider.Create(serializer));
        }
        finally
        {
            if (pool is not null)
                await pool.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Creates the serializer used by remote client sessions (metrics-decorated by default).</summary>
    /// <param name="serializer">Optional inner serializer; defaults to System.Text.Json.</param>
    /// <param name="enableMetrics">When <see langword="true" />, wraps the serializer with metrics recording.</param>
    /// <returns>Configured serializer instance.</returns>
    internal static ISquirixSerializer CreateSerializer(ISquirixSerializer? serializer = null, bool enableMetrics = true) =>
        SerializationProvider.Create(serializer, enableMetrics);

    private static CallCredentials? BuildCallCredentials(Func<CancellationToken, ValueTask<string>>? bearerTokenProvider)
    {
        if (bearerTokenProvider is null)
            return null;

        return new BearerTokenCallCredentials(bearerTokenProvider).Credentials;
    }

    private static string FormatEndpointNodeId(int index)
    {
        var digits = 1;
        for (var n = index; n >= 10; n /= 10)
            digits++;

        return string.Create(
            9 + digits,
            index,
            static (span, value) =>
            {
                "endpoint-".AsSpan().CopyTo(span);
                _ = value.TryFormat(span[9..], out _, provider: CultureInfo.InvariantCulture);
            });
    }

    private static Uri[] NormalizeEndpoints(IList<Uri> endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var buffer = new Uri[endpoints.Count];
        var count = 0;

        for (var index = 0; index < endpoints.Count; index++)
            _ = TryAddUniqueEndpoint(endpoints[index], seen, buffer, nameof(endpoints), ref count);

        return TrimEndpoints(buffer, count);
    }

    private static bool TryAddUniqueEndpoint(Uri? endpoint, HashSet<string> seen, Uri[] buffer, string paramName, ref int count)
    {
        var validated = RequireAbsoluteEndpoint(endpoint, paramName);
        GrpcTransportEndpoints.RequireHttps(validated);
        if (!seen.Add(validated.GetLeftPart(UriPartial.Authority)))
            return false;

        buffer[count++] = validated;
        return true;
    }

    private static Uri RequireAbsoluteEndpoint(Uri? endpoint, string paramName)
    {
        if (endpoint is null)
            throw new ArgumentException("Endpoint must be a non-null absolute URI.", paramName);

        if (!endpoint.IsAbsoluteUri || string.IsNullOrWhiteSpace(endpoint.Scheme) || string.IsNullOrWhiteSpace(endpoint.Host))
            throw new ArgumentException("Endpoint must be an absolute Squirix server URI.", paramName);

        return endpoint;
    }

    private static Uri[] TrimEndpoints(Uri[] buffer, int count)
    {
        if (count is 0)
            throw new InvalidOperationException("At least one Squirix server endpoint must be configured.");

        if (count == buffer.Length)
            return buffer;

        var trimmed = new Uri[count];
        buffer.AsSpan(0, count).CopyTo(trimmed);
        return trimmed;
    }

    private static class SerializationProvider
    {
        internal static ISquirixSerializer Create(ISquirixSerializer? serializer = null, bool enableMetrics = true)
        {
            var effective = serializer ?? new SystemTextJsonSerializer();
            return enableMetrics ? EnsureMetrics(effective) : effective;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ISquirixSerializer EnsureMetrics(ISquirixSerializer inner)
        {
            ArgumentNullException.ThrowIfNull(inner);
            return inner is MetricsDecoratedSerializer ? inner : new MetricsDecoratedSerializer(inner);
        }

        /// <summary>Decorator that records metrics for serialization operations and delegates to an inner serializer.</summary>
        [Immutable]
        private sealed class MetricsDecoratedSerializer : ISquirixSerializer
        {
            private readonly string _impl;

            private readonly ISquirixSerializer _inner;

            internal MetricsDecoratedSerializer(ISquirixSerializer inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
                _impl = _inner.GetType().Name;
            }

            public T? Deserialize<T>(string payload)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    var result = _inner.Deserialize<T>(payload);
                    Record(SerializerMetrics.OpDeserialize, true, start);
                    return result;
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpDeserialize, ex, start))
                {
                    throw;
                }
            }

            public T? Deserialize<T>(JsonElement payload)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    var result = _inner.Deserialize<T>(payload);
                    Record(SerializerMetrics.OpDeserialize, true, start);
                    return result;
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpDeserialize, ex, start))
                {
                    throw;
                }
            }

            public T? Deserialize<T>(ReadOnlySpan<byte> payload)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    var result = _inner.Deserialize<T>(payload);
                    Record(SerializerMetrics.OpDeserialize, true, start);
                    return result;
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpDeserialize, ex, start))
                {
                    throw;
                }
            }

            public T? Deserialize<T>(Stream payload)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    var result = _inner.Deserialize<T>(payload);
                    Record(SerializerMetrics.OpDeserialize, true, start);
                    return result;
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpDeserialize, ex, start))
                {
                    throw;
                }
            }

            public void Serialize<T>(Stream destination, T? value)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    _inner.Serialize(destination, value);
                    Record(SerializerMetrics.OpSerialize, true, start);
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpSerialize, ex, start))
                {
                    throw;
                }
            }

            public JsonElement SerializeToElement<T>(T? value)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    var result = _inner.SerializeToElement(value);
                    Record(SerializerMetrics.OpSerialize, true, start);
                    return result;
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpSerialize, ex, start))
                {
                    throw;
                }
            }

            public byte[] SerializeToUtf8Bytes<T>(T? value)
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    var result = _inner.SerializeToUtf8Bytes(value);
                    Record(SerializerMetrics.OpSerialize, true, start);
                    return result;
                }
                catch (Exception ex) when (TryRecordSerializerFailure(SerializerMetrics.OpSerialize, ex, start))
                {
                    throw;
                }
            }

            private void Record(string op, bool success, long startTimestamp)
            {
                var elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
                SerializerMetrics.OpsTotal.WithLabels(op, success ? "ok" : "error", _impl).Inc(1);
                SerializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
            }

            private void RecordFailure(string op, Exception ex, long startTimestamp)
            {
                var elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
                SerializerMetrics.OpsTotal.WithLabels(op, "error", _impl).Inc(1);
                SerializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
                var exType = ex.GetType().Name;
                SerializerMetrics.FailuresTotal.WithLabels(op, exType, _impl).Inc(1);
            }

            private bool TryRecordSerializerFailure(string op, Exception ex, long startTimestamp)
            {
                switch (ex)
                {
                    case JsonException:
                    case NotSupportedException:
                    case InvalidOperationException:
                    case IOException:
                        RecordFailure(op, ex, startTimestamp);
                        return true;
                    default:
                        return false;
                }
            }
        }
    }

    private sealed class BearerTokenCallCredentials
    {
        private const string AuthorizationHeader = "authorization";
        private const string BearerSchemePrefix = "Bearer ";

        private readonly Func<CancellationToken, ValueTask<string>> _tokenProvider;
        private string? _cachedAuthorizationHeader;
        private string? _cachedToken;

        internal BearerTokenCallCredentials(Func<CancellationToken, ValueTask<string>> tokenProvider)
        {
            _tokenProvider = tokenProvider;
            Credentials = CallCredentials.FromInterceptor(InterceptAsync);
        }

        internal CallCredentials Credentials { get; }

        private async Task AddAuthorizationHeaderAsync(ValueTask<string> tokenTask, Metadata metadata)
        {
            var token = await tokenTask.ConfigureAwait(false);
            var header = ResolveAuthorizationHeader(token);
            metadata.Add(AuthorizationHeader, header);
        }

        private Task InterceptAsync(AuthInterceptorContext context, Metadata metadata) => AddAuthorizationHeaderAsync(_tokenProvider(context.CancellationToken), metadata);

        private string ResolveAuthorizationHeader(string token)
        {
            var cachedToken = _cachedToken;
            var cachedHeader = _cachedAuthorizationHeader;
            if (cachedToken is not null && cachedHeader is not null && string.Equals(cachedToken, token, StringComparison.Ordinal))
                return cachedHeader;

            var header = string.Create(
                BearerSchemePrefix.Length + token.Length,
                (BearerSchemePrefix, token),
                static (span, state) =>
                {
                    state.BearerSchemePrefix.AsSpan().CopyTo(span);
                    state.token.AsSpan().CopyTo(span[state.BearerSchemePrefix.Length..]);
                });

            _cachedToken = token;
            _cachedAuthorizationHeader = header;
            return header;
        }
    }

    [Immutable]
    private sealed class RemoteClientSession : IRemoteClientSession
    {
        private readonly EndpointFailover _bootstrapFailover;
        private readonly IClientPool _remoteClients;
        private readonly ISquirixSerializer _serializer;

        internal RemoteClientSession(IClientPool remoteClients, EndpointFailover bootstrapFailover, ISquirixSerializer serializer)
        {
            _remoteClients = remoteClients ?? throw new ArgumentNullException(nameof(remoteClients));
            _bootstrapFailover = bootstrapFailover ?? throw new ArgumentNullException(nameof(bootstrapFailover));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public ValueTask DisposeAsync()
        {
            _remoteClients.BeginDrain();
            return _remoteClients.DisposeAsync();
        }

        public ICache<T> GetCache<T>(string cacheName) => new RemoteCache<T>(cacheName, _bootstrapFailover, _remoteClients, _serializer);
    }
}
