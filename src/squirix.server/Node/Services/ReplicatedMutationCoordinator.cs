using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Services;

/// <summary>Runs the mutating cache RPC handlers of a host whose writes commit through replica groups, which dedupe them themselves.</summary>
/// <remarks>
/// Every write of such a host commits through the group of its key, whose idempotency state is the single authority over the operation
/// identifier: it replays the recorded outcome of a retry, joins a duplicate that is in flight, refuses a reuse with another request, and
/// survives a leader change and a restart. The node-wide replay store is therefore not consulted, and no fingerprint is computed here; a
/// second authority could only disagree with the group.
/// <para>
/// The group identifies an operation by the replica group and the cache of its key, so an identifier reused with another request is
/// detected only within that group and cache; the same identifier on a key of another group or cache is a separate operation. A retry also
/// runs the whole handler chain again: only the group commit replays the recorded outcome, and a conditional write that took no effect is
/// evaluated again.
/// </para>
/// </remarks>
[Immutable]
internal sealed class ReplicatedMutationCoordinator : IRpcMutationIdempotencyCoordinator
{
    private readonly IJournalCoordinator? _journal;

    internal ReplicatedMutationCoordinator(IJournalCoordinator? journal)
    {
        _journal = journal;
    }

    /// <inheritdoc />
    public async Task<TResponse> ExecuteAsync<TState, TResponse>(
        string rawOperationId,
        Func<TState, string> fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        ArgumentNullException.ThrowIfNull(fingerprint);
        ArgumentNullException.ThrowIfNull(execute);

        _ = RpcMutationContracts.RequireOperationId(rawOperationId);

        // The group applies into memory through the local chain, which must be recovered first, as on the single-copy path.
        if (_journal != null)
            await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        return await execute(state, cancellationToken).ConfigureAwait(false);
    }
}
