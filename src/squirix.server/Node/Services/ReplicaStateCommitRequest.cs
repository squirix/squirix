using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>A <see cref="ReplicaCommitRequest" /> that carries the arguments of its write.</summary>
/// <typeparam name="TState">The arguments of the write.</typeparam>
[Immutable]
internal sealed class ReplicaStateCommitRequest<TState> : ReplicaCommitRequest
{
    private readonly Func<TState, byte[]> _fingerprint;
    private readonly Func<ReplicaMutationFactory, TState, ulong, CancellationToken, ValueTask<PreparedReplicaMutation>> _prepare;
    private readonly TState _state;

    /// <summary>Initializes a new instance of the <see cref="ReplicaStateCommitRequest{TState}" /> class.</summary>
    /// <param name="write">The cache scope, the client operation identifier, and the cache key of the write.</param>
    /// <param name="state">The arguments of the write.</param>
    /// <param name="fingerprint">Computes the operation fingerprint of the write from its arguments.</param>
    /// <param name="prepare">Prepares the mutation from the running factory, the arguments, and the log index it is appended at.</param>
    internal ReplicaStateCommitRequest(
        (string Scope, string OperationId, string Key) write,
        TState state,
        Func<TState, byte[]> fingerprint,
        Func<ReplicaMutationFactory, TState, ulong, CancellationToken, ValueTask<PreparedReplicaMutation>> prepare)
        : base(write.Scope, write.OperationId, write.Key)
    {
        ArgumentNullException.ThrowIfNull(fingerprint);
        ArgumentNullException.ThrowIfNull(prepare);
        _state = state;
        _fingerprint = fingerprint;
        _prepare = prepare;
    }

    /// <inheritdoc />
    /// <remarks>The fingerprint, which encodes and hashes the request, is computed only once an entry with the identity is found.</remarks>
    internal override GroupIdempotencyLookup LookupRetained(IFollowerLog? log, out GroupIdempotencyRecord record)
    {
        record = default;
        return log == null || log.Idempotency.Lookup(Scope, OperationId, [], out _) == GroupIdempotencyLookup.Miss
            ? GroupIdempotencyLookup.Miss
            : log.Idempotency.Lookup(Scope, OperationId, _fingerprint(_state), out record);
    }

    /// <inheritdoc />
    internal override ValueTask<PreparedReplicaMutation> PrepareAsync(ReplicaMutationFactory factory, ulong index, CancellationToken cancellationToken) =>
        _prepare(factory, _state, index, cancellationToken);
}
