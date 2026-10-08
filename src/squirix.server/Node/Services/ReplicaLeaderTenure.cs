using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>One leadership of a replica group won by election: its term, its leader-term entry, and the applier lease it holds.</summary>
/// <remarks>
/// The committer of the group writes the entry index and the authorization under its commit gate. The token ends with the leadership, so
/// the readiness loop of the leadership stops with it, and the lease goes back to the apply loop of the group at the same moment.
/// </remarks>
internal sealed class ReplicaLeaderTenure : IDisposable
{
    private readonly CancellationTokenSource _ending = new();
    private readonly ReplicaDriverLease _lease;
    private Type? _reportedFault;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLeaderTenure" /> class.</summary>
    /// <param name="term">The won term.</param>
    /// <param name="lease">The applier lease the leadership holds until it ends.</param>
    internal ReplicaLeaderTenure(ulong term, ReplicaDriverLease lease)
    {
        ArgumentOutOfRangeException.ThrowIfZero(term);
        ArgumentNullException.ThrowIfNull(lease);
        Term = term;
        _lease = lease;
        Token = _ending.Token;
    }

    /// <summary>Gets a value indicating whether the leader-term entry of the leadership is committed by one of its coordinators.</summary>
    internal bool Authorized { get; private set; }

    /// <summary>Gets the log index of the leader-term entry the leadership appended; zero before it is appended.</summary>
    internal ulong NoopIndex { get; private set; }

    /// <summary>Gets the won term.</summary>
    internal ulong Term { get; }

    /// <summary>Gets the token canceled when the leadership ends.</summary>
    internal CancellationToken Token { get; }

    /// <inheritdoc />
    /// <remarks>Releases the token source only; the lease stays with a leadership that did not end.</remarks>
    public void Dispose() => _ending.Dispose();

    /// <summary>Appends the leader-term entry after the last entry of the log, once per leadership.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="read">The leader tail read last.</param>
    /// <param name="factory">The mutation factory of the term.</param>
    /// <param name="leaderId">The identifier of this node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The leader tail after the append; <paramref name="read" /> when the entry was appended before.</returns>
    /// <exception cref="InvalidOperationException">The log refused the entry, as when it moved to a higher term.</exception>
    /// <remarks>
    /// Every leadership appends its own entry, at a new index with an identity of its own: no entry of an earlier leadership, committed or
    /// not, can stand in for it.
    /// </remarks>
    internal async Task<FollowerLogTail> AppendNoopAsync(IFollowerLog log, FollowerLogTail read, ReplicaMutationFactory factory, string leaderId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(factory);
        if (NoopIndex != 0)
            return read;

        var status = read.Status;
        var noop = factory.PrepareLeaderTerm(status.LastLogIndex + 1);
        FollowerLogEntry[] entries = [new(noop.LogIndex, noop.Term, noop.CanonicalPayload)];
        var request = new FollowerLogAppendRequest(leaderId, Term, status.LastLogIndex, status.LastLogTerm, status.CommitIndex, entries);
        var appended = await log.AppendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!appended.Success)
            throw new InvalidOperationException($"The group log refused the leader-term entry of term {Term}: {appended.RefusalCode}.");

        NoopIndex = noop.LogIndex;
        return await log.GetLeaderTailAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Ends the leadership: its token is canceled and the applier lease goes back to the apply loop.</summary>
    /// <returns>An asynchronous operation.</returns>
    internal async ValueTask EndAsync()
    {
        await _ending.CancelAsync().ConfigureAwait(false);
        _lease.EndLeading();
        _ending.Dispose();
    }

    /// <summary>Tells whether the leader-term entry is committed by a coordinator of this leadership and the log still holds it in the term.</summary>
    /// <param name="coordinator">The running coordinator, started in this leadership.</param>
    /// <param name="log">The group log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when the leadership is authorized; once it is, it stays so for the rest of the leadership.</returns>
    /// <remarks>
    /// Runs under the commit gate. A coordinator of this leadership moves its commit index past the entry only once verified followers of
    /// the term hold it; a durable commit index raised by any other leader comes with a higher term, which the term checks refuse.
    /// </remarks>
    internal async Task<bool> IsAuthorizedAsync(ReplicaCommitCoordinator? coordinator, IFollowerLog log, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        if (Authorized)
            return true;
        if (NoopIndex == 0 || coordinator == null || coordinator.CommitIndex < NoopIndex)
            return false;

        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        Authorized = status.CurrentTerm == Term && await log.GetTermAtAsync(NoopIndex, cancellationToken).ConfigureAwait(false) == Term;
        return Authorized;
    }

    /// <summary>Records the kind of fault the last start of the leadership failed with, or that it succeeded.</summary>
    /// <param name="fault">The type of the fault; <see langword="null" /> after a start that succeeded.</param>
    /// <returns><see langword="true" /> when it differs from the fault recorded last, so it is to be logged.</returns>
    /// <remarks>Called under the commit gate.</remarks>
    internal bool ReportFault(Type? fault)
    {
        var changed = _reportedFault != fault;
        _reportedFault = fault;
        return changed;
    }

    /// <summary>Gets the term a start of the leadership leads in.</summary>
    /// <param name="status">The log status read at the start.</param>
    /// <param name="groupId">The group identifier, for the refusal.</param>
    /// <returns>The won term.</returns>
    /// <exception cref="InvalidOperationException">The log moved past the won term: a newer leader exists.</exception>
    internal ulong TermFor(in FollowerLogStatus status, string groupId) => status.CurrentTerm <= Term ? Term
        : throw new InvalidOperationException($"Replica group '{groupId}' moved to term {status.CurrentTerm} past the led term {Term}.");
}
