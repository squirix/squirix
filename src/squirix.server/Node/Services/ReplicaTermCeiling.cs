using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Reads the term ceiling a leader needs before it repairs a member that lost its disk (#1126).</summary>
/// <remarks>
/// <para>
/// The leader asks every other member, without the wiped one, for its durable current term. With the leader itself, the members that answered
/// from a copy that kept its term must form a majority of the group: both others at three replicas, three of the four others at five. A copy
/// that lost its disk too has no term to tell and is not counted.
/// </para>
/// <para>
/// Why it is enough: a vote in a term was asked by a candidate that holds that term durably, and a candidate that won it is held at that term by a
/// majority. The answers and the leader form a majority as well, so they meet every majority that elected someone with the wiped member's lost vote:
/// some answer carries a term at least that high. When no answer is above the leader's term, the wiped member never helped elect anyone in a higher
/// term, and recording its vote for the leader in the leader's term keeps it from voting twice there.
/// </para>
/// </remarks>
[Immutable]
internal sealed class ReplicaTermCeiling
{
    private readonly IReplicaBootstrapGateway _gateway;
    private readonly TimeSpan _timeout;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTermCeiling" /> class.</summary>
    /// <param name="gateway">The status RPCs to the members.</param>
    /// <param name="timeout">The budget of each status call.</param>
    internal ReplicaTermCeiling(IReplicaBootstrapGateway gateway, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(gateway);
        _gateway = gateway;
        _timeout = timeout;
    }

    /// <summary>Reads the current terms of the members other than the leader and the wiped one.</summary>
    /// <param name="members">The members of the group by replica slot.</param>
    /// <param name="header">The envelope of the leader; its term is the term the repair would record.</param>
    /// <param name="wipedSlot">The slot of the member that lost its disk.</param>
    /// <param name="leaderSlot">The slot of the leader.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation propagates.</param>
    /// <returns>How many members answered with a term, how many the majority needs, and the highest term they told.</returns>
    internal async Task<Read> ReadAsync(string[] members, ReplicaRpcHeader header, int wipedSlot, int leaderSlot, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(members);
        var calls = new List<Task<ReplicaMemberStatus?>>(members.Length);
        for (var slot = 0; slot < members.Length; slot++)
        {
            if (slot != wipedSlot && slot != leaderSlot)
                calls.Add(StatusAsync(members[slot], header, cancellationToken));
        }

        var statuses = await Task.WhenAll(calls).ConfigureAwait(false);
        var answers = 0;
        var highest = 0UL;
        foreach (var status in statuses)
        {
            if (status is not { RefusalCode.Length: 0, MemberState: GroupMemberState.Complete or GroupMemberState.Incomplete } answered)
                continue;

            answers++;
            highest = Math.Max(highest, answered.Term);
        }

        // A majority of the group, without the leader, which answers for itself.
        return new Read(answers, members.Length / 2, highest);
    }

    private async Task<ReplicaMemberStatus?> StatusAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(_timeout);
        try
        {
            return await _gateway.GetStatusAsync(nodeId, header, bounded.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception exception) when (exception is RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException)
        {
            // A member that cannot be reached tells no term; the ceiling then waits for one that can.
            return null;
        }
    }

    /// <summary>The terms the members told.</summary>
    /// <param name="Answers">How many members answered with the term of a copy that kept it.</param>
    /// <param name="Needed">How many answers make a majority with the leader, without the wiped member.</param>
    /// <param name="Highest">The highest term among the answers; zero when none answered.</param>
    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    internal readonly record struct Read(int Answers, int Needed, ulong Highest)
    {
        /// <summary>Gets a value indicating whether enough members answered for the ceiling to hold.</summary>
        internal bool Suffices => Answers >= Needed;
    }
}
