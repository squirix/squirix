using System;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The observations of <see cref="ReplicaGroupCommitter" /> that the election driver and memory admission read without the commit gate.</summary>
internal static class ReplicaGroupCommitterSignals
{
    extension(ReplicaGroupCommitter committer)
    {
        /// <summary>Reports how far the applier is behind the entries a majority backs, without taking the commit gate.</summary>
        /// <returns>The progress; caught up when no coordinator runs.</returns>
        /// <remarks>
        /// The applied index is clamped to the majority index, so an applier that is ahead of a fresh coordinator never reads as behind. Called by
        /// the election driver, which must not wait for a gate that a blocked apply holds.
        /// </remarks>
        internal ReplicaApplyProgress ApplyProgress()
        {
            if (committer.ReadCoordinator() is not { } coordinator)
                return default;

            var majority = coordinator.MajorityIndex;
            return new ReplicaApplyProgress(Math.Min(committer.Applier.AppliedIndex, majority), majority);
        }

        /// <summary>Tells whether a member of the group recently reported critical memory pressure, so the group must not grow.</summary>
        /// <returns><see langword="true" /> while a member's flag is within its lifetime.</returns>
        /// <remarks>
        /// Called outside the commit gate; it never waits and never throws. A static leader sends no heartbeats of its own, so when a refusal
        /// finds the flag older than half its lifetime it asks the followers for a fresh reply, without waiting for it: a member that has
        /// recovered clears the flag, and one that has not keeps it fresh.
        /// </remarks>
        internal bool HasMemberPressure()
        {
            var pressure = committer.MemberPressure;
            if (!pressure.IsAnyFresh())
                return false;

            if (committer.Election == null && pressure.NeedsRefresh())
                committer.RunningPipeline?.Heartbeat(true);

            return true;
        }
    }
}
