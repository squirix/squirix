using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>Searches bounded delivery, loss and clock orderings for a leader that drops valid probe contacts.</summary>
internal static class QuorumContactSearch
{
    internal const int MaxStates = 50_000;

    /// <summary>Two log-mismatch replies in flight; the other followers are silent.</summary>
    private const int PendingReplies = (1 << 1) | (1 << 2);

    internal static (QuorumContactState Healthy, QuorumContactState Broken, int SeenStates) FindDroppedContact()
    {
        var initial = QuorumContactState.Start(5);
        var start = new SearchState(initial, initial, PendingReplies);
        var seen = new HashSet<SearchState> { start };
        var pending = new Queue<SearchState>();
        pending.Enqueue(start);

        while (pending.TryDequeue(out var state))
        {
            var healthy = state.Healthy.CheckQuorum();
            var broken = state.Broken.CheckQuorum();
            if (healthy.Role == NodeRole.Leader && broken.Role == NodeRole.Follower)
                return (healthy, broken, seen.Count);

            // A check that leaves both leaders alone has no effect on the search state.
            // A check that demotes both ends this tenure, so it cannot distinguish the modes later.
            if (state.Healthy.Tick < QuorumContactState.MaxTick)
                Add(state with { Healthy = state.Healthy.Advance(), Broken = state.Broken.Advance() });

            for (var slot = 1; slot <= 2; slot++)
            {
                var bit = 1 << slot;
                if ((state.Pending & bit) == 0)
                    continue;

                var remaining = state with { Pending = state.Pending & ~bit };
                var reply = new QuorumProbeReply(slot, initial.Term, initial.Term, QuorumProbeVerdict.LogMismatch);
                Add(remaining with { Healthy = remaining.Healthy.Receive(reply), Broken = remaining.Broken.Receive(reply, true) });
                Add(remaining);
            }
        }

        throw new InvalidOperationException("No lost probe contact was found within the declared projection.");

        void Add(SearchState next)
        {
            if (!seen.Add(next))
                return;

            if (seen.Count > MaxStates)
                throw new InvalidOperationException("The quorum contact search exceeded its state budget.");

            pending.Enqueue(next);
        }
    }

    [StructLayout(LayoutKind.Auto)]
    [Immutable]
    private readonly record struct SearchState(QuorumContactState Healthy, QuorumContactState Broken, int Pending);
}
