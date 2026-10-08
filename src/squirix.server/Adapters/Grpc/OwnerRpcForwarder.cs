using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Sends an inbound single-key RPC unchanged from the entry node to the key owner and returns the owner response as is.</summary>
/// <remarks>
/// The entry node takes its own backpressure admission for the duration of the hop, including any slowdown delay and queue wait, but reserves
/// no operation id, runs no cache pipeline and writes no journal record: only the owner does. The owner admits the forwarded call only to a
/// free slot and refuses it at once otherwise, so a forwarded request waits in at most one admission queue and nodes never wait on each
/// other. The request instance is sent as parsed, so the owner computes the same fingerprint as for a direct call.
/// </remarks>
[Immutable]
internal sealed class OwnerRpcForwarder
{
    private readonly IBackpressureClientIdResolver _clientIdResolver;
    private readonly IBackpressureGate _gate;
    private readonly IServerClientPool _pool;
    private readonly RingAgreement _ringAgreement;

    internal OwnerRpcForwarder(IServerClientPool pool, IBackpressureGate gate, IBackpressureClientIdResolver clientIdResolver, RingAgreement ringAgreement)
    {
        ArgumentNullException.ThrowIfNull(pool);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(clientIdResolver);
        ArgumentNullException.ThrowIfNull(ringAgreement);
        _pool = pool;
        _gate = gate;
        _clientIdResolver = clientIdResolver;
        _ringAgreement = ringAgreement;
    }

    internal Task<GetEntryAsyncResponse> GetEntryAsync(string owner, GetEntryAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.GetEntry,
        request,
        static (client, r, ct) => client.GetEntryAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<GetExpirationAsyncResponse> GetExpirationAsync(string owner, GetExpirationAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.GetEntry,
        request,
        static (client, r, ct) => client.GetExpirationAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<GetOrAddAsyncResponse> GetOrAddAsync(string owner, GetOrAddAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.TryAdd,
        request,
        static (client, r, ct) => client.GetOrAddAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<GetValueAsyncResponse> GetValueAsync(string owner, GetValueAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Get,
        request,
        static (client, r, ct) => client.GetValueAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<RemoveAsyncResponse> RemoveAsync(string owner, RemoveAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Remove,
        request,
        static (client, r, ct) => client.RemoveAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<RemoveExpirationAsyncResponse> RemoveExpirationAsync(string owner, RemoveExpirationAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.RemoveExpiration,
        request,
        static (client, r, ct) => client.RemoveExpirationAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<SetAsyncResponse> SetEntryAsync(string owner, SetEntryAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Set,
        request,
        static (client, r, ct) => client.SetEntryAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<TouchAsyncResponse> TouchAsync(string owner, TouchAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Touch,
        request,
        static (client, r, ct) => client.TouchAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<TryAddAsyncResponse> AddEntryIfAbsentAsync(string owner, TryAddEntryAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.TryAdd,
        request,
        static (client, r, ct) => client.TryAddEntryAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    internal Task<UpdateAsyncResponse> UpdateAsync(string owner, UpdateAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Update,
        request,
        static (client, r, ct) => client.UpdateAsync(r, cancellationToken: ct).ResponseAsync,
        cancellationToken);

    private static RpcException CreatePoolDisposedUnavailable() => new(new Status(StatusCode.Unavailable, "ServerPeer client pool is disposed."));

    /// <summary>Copies an owner failure so it reaches the caller with its status, detail and application trailers, minus the reserved protocol trailers.</summary>
    /// <param name="ex">The owner failure.</param>
    /// <returns>The exception to throw to the caller.</returns>
    private static RpcException RelayOwnerFailure(RpcException ex)
    {
        var trailers = new Metadata();
        for (var i = 0; i < ex.Trailers.Count; i++)
        {
            var entry = ex.Trailers[i];
            if (entry.Key.StartsWith("grpc-", StringComparison.OrdinalIgnoreCase))
                continue;

            if (entry.IsBinary)
                trailers.Add(entry.Key, entry.ValueBytes);
            else
                trailers.Add(entry.Key, entry.Value);
        }

        return new RpcException(ex.Status, trailers);
    }

    private async Task<TResponse> ForwardAsync<TRequest, TResponse>(
        string owner,
        string operation,
        TRequest request,
        Func<SquirixCacheService.SquirixCacheServiceClient, TRequest, CancellationToken, Task<TResponse>> call,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await _gate.AcquireAsync(CacheOperationNames.CacheTransport, operation, _clientIdResolver.Resolve(), cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
        {
            try
            {
                // A concurrent pool disposal can dispose the policy between the lookups and the execution; both surface as the pool-disposed failure.
                var client = _pool.ForNode(owner);
                return await _pool.PolicyFor(owner).ExecuteAsync(
                    (Client: client, Request: request, Call: call),
                    static (s, ct) => new ValueTask<TResponse>(s.Call(s.Client, s.Request, ct)),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                throw CreatePoolDisposedUnavailable();
            }
            catch (RpcException ex)
            {
                // Only a failure of this client carries its cause; a status the owner sent never does. A failed connect sent nothing.
                if (ex.StatusCode == StatusCode.Unavailable && OwnerUnreachableFailure.IsConnectFailure(ex.Status.DebugException))
                    throw OwnerUnreachableFailure.Create(ex.Status.DebugException!);

                // The owner refused because its ring differs from this node: fence this node too, then relay the refusal with its trailers.
                if (RingMismatchFailure.IsMismatch(ex))
                    _ringAgreement.ReportOutboundMismatch(owner);

                throw RelayOwnerFailure(ex);
            }
            catch (Exception ex) when (ex is HttpRequestException or IOException)
            {
                // Any other transport failure may follow a request the owner already received, so it is not reported as unreachable.
                throw OwnerUnreachableFailure.IsConnectFailure(ex)
                    ? OwnerUnreachableFailure.Create(ex)
                    : new RpcException(new Status(StatusCode.Unavailable, $"The connection to key owner '{owner}' failed after the call may have reached it."));
            }
        }
    }
}
