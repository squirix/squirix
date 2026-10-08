using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Fixed configuration of the replica group elections: the timeouts, the heartbeat, and the jitter that breaks split votes.</summary>
/// <remarks>
/// The defaults keep a cluster whose nodes start a few hundred milliseconds apart in its provisional term: an election timeout of half a
/// second deposed the owner of a group before its peers answered and elected a needless second term in half of the measured three-node
/// starts, while one second elected none. A failover then waits one to two seconds for the timeout and the jitter before it campaigns.
/// </remarks>
[Immutable]
internal sealed class ElectionTimerOptions
{
    private readonly TimeSpan? _leaderWaitTimeout;

    /// <summary>Gets the time without leader contact after which a follower starts an election; also the window of the leader quorum check.</summary>
    internal TimeSpan ElectionTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Gets the time between two heartbeats of a leader; it must stay well below <see cref="ElectionTimeout" />.</summary>
    internal TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Gets the seed of the election jitter; every group mixes its identifier in, so groups draw different delays.</summary>
    /// <remarks>Drawn once per options instance unless set, so nodes differ while a test can pin it for a deterministic run.</remarks>
    internal ulong JitterSeed { get; init; } = BinaryPrimitives.ReadUInt64LittleEndian(RandomNumberGenerator.GetBytes(sizeof(ulong)));

    /// <summary>Gets the longest wait of an entry node for a leader of a served group that has none known.</summary>
    /// <remarks>
    /// Defaults to <see cref="ElectionTimeout" /> plus <see cref="MaxJitter" />, the longest a follower waits before it campaigns; the remaining
    /// deadline of the request caps it further.
    /// </remarks>
    internal TimeSpan LeaderWaitTimeout
    {
        get => _leaderWaitTimeout ?? (ElectionTimeout + MaxJitter);
        init => _leaderWaitTimeout = value;
    }

    /// <summary>Gets the largest random delay added to <see cref="ElectionTimeout" /> each time a follower arms its election.</summary>
    internal TimeSpan MaxJitter { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Gets the longest wait for one pre-vote or vote reply; an unanswered voter counts as a refusal.</summary>
    internal TimeSpan VoteRpcTimeout { get; init; } = TimeSpan.FromMilliseconds(250);
}
