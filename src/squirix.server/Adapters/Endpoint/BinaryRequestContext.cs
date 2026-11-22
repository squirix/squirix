using System;
using System.Security.Claims;
using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Metadata carried from the listener into authorization and cache dispatch.</summary>
internal sealed class BinaryRequestContext
{
    private const int RouteWriting = 1;
    private const int RouteReady = 2;
    private string? _routedCacheName;
    private string? _routedGroupId;
    private string? _routedKey;
    private ServerKeyValidationError? _routedKeyError;
    private int _routeState;

    internal required string Path { get; init; }

    internal IServiceProvider? RequestServices { get; init; }

    internal string? ConnectionId { get; init; }

    internal BinaryConnection? Connection { get; init; }

    internal ClaimsPrincipal? User { get; set; }

    internal CancellationToken RequestAborted { get; init; }

    internal string? TraceParent { get; set; }

    internal string? TraceState { get; set; }

    internal string? ClientId { get; set; }

    internal void RecordRoute(string cacheName, string key, ServerKeyValidationError? error, string? groupId)
    {
        if (Interlocked.CompareExchange(ref _routeState, RouteWriting, 0) != 0)
            return;

        _routedCacheName = cacheName;
        _routedKey = key;
        _routedKeyError = error;
        _routedGroupId = groupId;
        Volatile.Write(ref _routeState, RouteReady);
    }

    internal bool TryGetKeyValidation(string? key, out ServerKeyValidationError? error)
    {
        if (Volatile.Read(ref _routeState) == RouteReady && ReferenceEquals(_routedKey, key))
        {
            error = _routedKeyError;
            return true;
        }

        error = null;
        return false;
    }

    internal string? FindGroup(string cacheName, string key) =>
        Volatile.Read(ref _routeState) == RouteReady &&
        ReferenceEquals(_routedKey, key) &&
        string.Equals(_routedCacheName, cacheName, StringComparison.Ordinal)
            ? _routedGroupId
            : null;
}
