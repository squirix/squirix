using System;
using System.Text;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Canonical append-request builder shared by replica persistence tests.</summary>
internal static class FollowerFoundationScenario
{
    /// <summary>Builds a single-entry append request with the canonical follower-log shape.</summary>
    /// <param name="leaderNodeId">The leader node identifier used as the request's leader.</param>
    /// <param name="index">The journal index of the appended entry.</param>
    /// <param name="term">The term of the appended entry.</param>
    /// <param name="payload">The entry payload text.</param>
    /// <returns>The append request carrying one <see cref="FollowerLogEntry" />.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="index" /> is zero.</exception>
    /// <remarks>
    /// The declared predecessor term equals <paramref name="term" />, except for <paramref name="index" /> 1, where
    /// it is zero because no entry exists at index 0. Use this helper only for a chain of entries that share one
    /// term. For a term boundary, build the <see cref="FollowerLogAppendRequest" /> directly.
    /// The request's <c language="csharp">PrevLogIndex</c> is always <c language="csharp">index - 1UL</c>, so a batch with a non-contiguous
    /// predecessor cannot be expressed through this helper; build the request directly for that case.
    /// </remarks>
    internal static FollowerLogAppendRequest Append(string leaderNodeId, ulong index, ulong term, string payload)
    {
        ArgumentOutOfRangeException.ThrowIfEqual(index, 0UL);

        return new FollowerLogAppendRequest(
            leaderNodeId,
            term,
            index - 1UL,
            index == 1UL ? 0UL : term,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))]));
    }
}
