using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Wire;

/// <summary>Sends an inbound single-key RPC unchanged from the entry node to the key owner and returns the owner response as is.</summary>
/// <remarks>
/// The entry node takes its own backpressure admission for the duration of the hop, including any slowdown delay and queue wait, but reserves
/// no replay outcome, runs no cache pipeline and writes no journal record: only the owner does. The owner admits the forwarded call only to a
/// free slot and refuses it at once otherwise, so a forwarded request waits in at most one admission queue and nodes never wait on each
/// other. The request instance is sent as parsed.
/// </remarks>
[Immutable]
internal sealed class OwnerRpcForwarder
{
    private readonly IBackpressureClientIdResolver _clientIdResolver;
    private readonly IBackpressureGate _gate;
    private readonly bool _mutationReplay;
    private readonly IServerClientPool _pool;
    private readonly RingAgreement _ringAgreement;

    internal OwnerRpcForwarder(IServerClientPool pool, IBackpressureGate gate, IBackpressureClientIdResolver clientIdResolver, RingAgreement ringAgreement, bool mutationReplay = false)
    {
        ArgumentNullException.ThrowIfNull(pool);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(clientIdResolver);
        ArgumentNullException.ThrowIfNull(ringAgreement);
        _pool = pool;
        _gate = gate;
        _clientIdResolver = clientIdResolver;
        _ringAgreement = ringAgreement;
        _mutationReplay = mutationReplay;
    }

    internal Task<TryAddAsyncResponse> AddEntryIfAbsentAsync(string owner, TryAddEntryAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.TryAdd,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.TryAddEntry, r, TryAddAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<GetDistributedAsyncResponse> GetDistributedAsync(string owner, GetDistributedAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Get,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.GetDistributed, r, GetDistributedAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<GetEntryAsyncResponse> GetEntryAsync(string owner, GetEntryAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.GetEntry,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.GetEntry, r, GetEntryAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<GetExpirationAsyncResponse> GetExpirationAsync(string owner, GetExpirationAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.GetEntry,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.GetExpiration, r, GetExpirationAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<GetOrAddAsyncResponse> GetOrAddAsync(string owner, GetOrAddAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.TryAdd,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.GetOrAdd, r, GetOrAddAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<GetValueAsyncResponse> GetValueAsync(string owner, GetValueAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Get,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.GetValue, r, GetValueAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<RefreshDistributedAsyncResponse> RefreshDistributedAsync(string owner, RefreshDistributedAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Touch,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.RefreshDistributed, r, RefreshDistributedAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<RemoveAsyncResponse> RemoveAsync(string owner, RemoveAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Remove,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.Remove, r, RemoveAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<RemoveExpirationAsyncResponse> RemoveExpirationAsync(string owner, RemoveExpirationAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.RemoveExpiration,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.RemoveExpiration, r, RemoveExpirationAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<SetAsyncResponse> SetDistributedAsync(string owner, SetDistributedAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Set,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.SetDistributed, r, SetAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<SetAsyncResponse> SetEntryAsync(string owner, SetEntryAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Set,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.SetEntry, r, SetAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<TouchAsyncResponse> TouchAsync(string owner, TouchAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Touch,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.Touch, r, TouchAsyncResponse.Parser, ct),
        cancellationToken);

    internal Task<UpdateAsyncResponse> UpdateAsync(string owner, UpdateAsyncRequest request, CancellationToken cancellationToken) => ForwardAsync(
        owner,
        CacheOperationNames.Update,
        request,
        static (client, r, ct) => client.SendAsync(PeerOperation.Update, r, UpdateAsyncResponse.Parser, ct),
        cancellationToken);

    private static ServerCallException CreatePoolDisposedUnavailable() => new(FrameStatus.Unavailable, "ServerPeer client pool is disposed.");

    private async Task<TResponse> ForwardAsync<TRequest, TResponse>(
        string owner,
        string operation,
        TRequest request,
        Func<IPeerClient, TRequest, CancellationToken, Task<TResponse>> call,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await InboundAdmissionScope.AcquireIfNeededAsync(_gate, CacheOperationNames.CacheTransport, operation, _clientIdResolver, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown");

        // The call policy may make several attempts and rethrows only the last failure, so every attempt is recorded: a forward is reported as
        // unreachable only when no attempt connected.
        var preventReplay = !_mutationReplay && request is not (GetEntryAsyncRequest or GetExpirationAsyncRequest or GetValueAsyncRequest or GetDistributedAsyncRequest);
        var attempts = new ConnectAttempts(preventReplay);
        using (lease)
        {
            try
            {
                // A concurrent pool disposal can dispose the policy between the lookups and the execution; both surface as the pool-disposed failure.
                var client = _pool.ForNode(owner);
                return await _pool.PolicyFor(owner).ExecuteAsync(
                    (Client: client, Request: request, Call: call, Attempts: attempts),
                    static (s, ct) => new ValueTask<TResponse>(s.Attempts.RunAsync(s.Call, s.Client, s.Request, ct)),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                throw CreatePoolDisposedUnavailable();
            }
            catch (ServerCallException ex)
            {
                // Only a failure of this client carries its cause; a status the owner sent never does. A failed connect sent nothing, whatever
                // status the client gave it: a failed TLS handshake surfaces as Internal, not Unavailable.
                if (attempts.NoneConnected && OwnerUnreachableFailure.IsConnectFailure(ex.InnerException))
                    throw OwnerUnreachableFailure.Create(ex.InnerException!);

                // The owner refused because its ring differs from this node: fence this node too, then relay the refusal with its trailers.
                if (RingMismatchFailure.IsMismatch(ex))
                    _ringAgreement.ReportOutboundMismatch(owner);

                throw;
            }
            catch (Exception ex) when (ex is HttpRequestException or IOException)
            {
                // Any other transport failure may follow a request the owner already received, so it is not reported as unreachable.
                throw attempts.NoneConnected && OwnerUnreachableFailure.IsConnectFailure(ex)
                    ? OwnerUnreachableFailure.Create(ex)
                    : new ServerCallException(FrameStatus.Unavailable, $"The connection to key owner '{owner}' failed after the call may have reached it.");
            }
        }
    }

    /// <summary>Records whether any attempt of one forward failed other than by failing to connect, so the request may have reached the target.</summary>
    /// <remarks>The call policy runs the attempts one after another; the flag only ever goes from unset to set.</remarks>
    [ThreadSafe]
    private sealed class ConnectAttempts
    {
        private readonly bool _preventReplay;
        private int _connected;

        internal ConnectAttempts(bool preventReplay)
        {
            _preventReplay = preventReplay;
        }

        /// <summary>Gets a value indicating whether every failed attempt so far failed to connect, so none of them sent the request.</summary>
        internal bool NoneConnected => Volatile.Read(ref _connected) == 0;

        /// <summary>Runs one attempt and records a failure that may follow a sent request.</summary>
        /// <typeparam name="TRequest">The request type.</typeparam>
        /// <typeparam name="TResponse">The response type.</typeparam>
        /// <param name="call">Starts the call.</param>
        /// <param name="client">The client of the target.</param>
        /// <param name="request">The request.</param>
        /// <param name="cancellationToken">The attempt cancellation token.</param>
        /// <returns>The response.</returns>
        /// <exception cref="ServerCallException">The peer call failed.</exception>
        internal async Task<TResponse> RunAsync<TRequest, TResponse>(
            Func<IPeerClient, TRequest, CancellationToken, Task<TResponse>> call,
            IPeerClient client,
            TRequest request,
            CancellationToken cancellationToken)
        {
            try
            {
                return await call(client, request, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!OwnerUnreachableFailure.IsConnectFailure(ex is ServerCallException rpc ? rpc.InnerException : ex))
            {
                Volatile.Write(ref _connected, 1);
                if (_preventReplay && IsOutcomeUncertain(ex))
                    throw ServerOpContract.CommitOutcomeUnknown().ToServerCallException();
                throw;
            }
        }

        private static bool IsOutcomeUncertain(Exception error) => error switch
        {
            ServerCallException rpc =>
                rpc.StatusCode is FrameStatus.Unavailable or FrameStatus.Cancelled or FrameStatus.DeadlineExceeded or FrameStatus.Internal
                && !RingMismatchFailure.IsRefusal(rpc)
                && !StaleRouteSignals.TryReadStale(rpc, out _)
                && !string.Equals(rpc.Message, ServerOpContract.NoLeaderAuthorityDetail, StringComparison.Ordinal),
            OperationCanceledException or HttpRequestException or IOException => true,
            _ => false,
        };
    }
}
