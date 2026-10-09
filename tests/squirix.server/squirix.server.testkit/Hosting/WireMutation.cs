using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// One logical cache mutation with a fixed operation identifier. The SDK stamps a new identifier on every logical call and offers no way to
/// choose it, so a test that sends one logical operation again, as the SDK does on a retry or a reroute, builds the mutation once and sends
/// the same instance through <see cref="WireClient" /> as often as it needs.
/// </summary>
[Immutable]
internal sealed class WireMutation
{
    private readonly Func<SquirixCacheService.SquirixCacheServiceClient, DateTime, CancellationToken, Task<WireOutcome>> _send;

    private WireMutation(string operationId, Func<SquirixCacheService.SquirixCacheServiceClient, DateTime, CancellationToken, Task<WireOutcome>> send)
    {
        OperationId = operationId;
        _send = send;
    }

    /// <summary>Gets the operation identifier every send of the mutation carries.</summary>
    internal string OperationId { get; }

    /// <summary>Builds a GetOrAdd under a new operation identifier.</summary>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value added when the key is absent.</param>
    /// <returns>The mutation.</returns>
    internal static WireMutation GetOrAdd(string cacheName, string key, string value)
    {
        var request = new GetOrAddAsyncRequest { OperationId = NewOperationId(), CacheName = Canonical(cacheName), Key = key, Entry = Entry(value) };
        return new WireMutation(request.OperationId, (client, deadline, cancellationToken) => GetOrAddAsync(client, request, deadline, cancellationToken));
    }

    /// <summary>Builds a Set under a new operation identifier.</summary>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <returns>The mutation.</returns>
    internal static WireMutation Set(string cacheName, string key, string value)
    {
        var request = new SetEntryAsyncRequest { OperationId = NewOperationId(), CacheName = Canonical(cacheName), Key = key, Entry = Entry(value) };
        return new WireMutation(request.OperationId, (client, deadline, cancellationToken) => SetAsync(client, request, deadline, cancellationToken));
    }

    /// <summary>Builds an add-if-absent under a new operation identifier.</summary>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">The key.</param>
    /// <param name="value">The value added when the key is absent.</param>
    /// <returns>The mutation.</returns>
    internal static WireMutation AddIfAbsent(string cacheName, string key, string value)
    {
        var request = new TryAddEntryAsyncRequest { OperationId = NewOperationId(), CacheName = Canonical(cacheName), Key = key, Entry = Entry(value) };
        return new WireMutation(request.OperationId, (client, deadline, cancellationToken) => AddIfAbsentAsync(client, request, deadline, cancellationToken));
    }

    /// <summary>Sends the mutation once.</summary>
    /// <param name="client">The generated client of the node.</param>
    /// <param name="deadline">The point in time, in UTC, by which the call must end.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task whose result is the answer of the node; it fails with the status of the call.</returns>
    internal Task<WireOutcome> SendAsync(SquirixCacheService.SquirixCacheServiceClient client, DateTime deadline, CancellationToken cancellationToken) =>
        _send(client, deadline, cancellationToken);

    private static string Canonical(string cacheName) =>
        ServerCacheName.TryParsePublic(cacheName, out var canonical) ? canonical : throw new ArgumentException($"'{cacheName}' is not a valid cache name.", nameof(cacheName));

    private static CacheEntryWire Entry(string value) => new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto();

    private static async Task<WireOutcome> GetOrAddAsync(SquirixCacheService.SquirixCacheServiceClient client, GetOrAddAsyncRequest request, DateTime deadline, CancellationToken cancellationToken)
    {
        var response = await client.GetOrAddAsync(request, deadline: deadline, cancellationToken: cancellationToken).ConfigureAwait(false);
        return new WireOutcome(response.Added, response.Found ? await ServerProtoEx.MapCacheValueAsync<string>(response.Value).ConfigureAwait(false) : null);
    }

    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    private static async Task<WireOutcome> SetAsync(SquirixCacheService.SquirixCacheServiceClient client, SetEntryAsyncRequest request, DateTime deadline, CancellationToken cancellationToken)
    {
        _ = await client.SetEntryAsync(request, deadline: deadline, cancellationToken: cancellationToken).ConfigureAwait(false);
        return new WireOutcome(true, null);
    }

    private static async Task<WireOutcome> AddIfAbsentAsync(SquirixCacheService.SquirixCacheServiceClient client, TryAddEntryAsyncRequest request, DateTime deadline, CancellationToken cancellationToken)
    {
        var response = await client.TryAddEntryAsync(request, deadline: deadline, cancellationToken: cancellationToken).ConfigureAwait(false);
        return new WireOutcome(response.Added, null);
    }
}
