using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Election timing of the nodes of a test cluster: the timeouts, the heartbeat and the jitter of the replica group elections. The defaults are
/// the pull request tier: a two-second election timeout, one second of jitter, a 200 ms heartbeat and a one-second vote wait.
/// </summary>
/// <remarks>Every node of a cluster should use the same timing. A node started without one runs the product defaults.</remarks>
[Immutable]
public sealed record TestElectionTiming
{
    /// <summary>Gets the time without leader contact after which a follower starts an election.</summary>
    public TimeSpan ElectionTimeout { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>Gets the time between two heartbeats of a leader; it must stay below <see cref="ElectionTimeout" />.</summary>
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Gets the seed of the election jitter, combined with the node identifier so the nodes still draw different delays; when
    /// <see langword="null" />, every node draws a random seed.
    /// </summary>
    public ulong? JitterSeed { get; init; }

    /// <summary>Gets the longest wait of an entry node for a leader; when <see langword="null" />, the election timeout plus the jitter.</summary>
    public TimeSpan? LeaderWaitTimeout { get; init; }

    /// <summary>Gets the largest random delay added to <see cref="ElectionTimeout" /> each time a follower arms its election.</summary>
    public TimeSpan MaxJitter { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Gets the longest wait for one pre-vote or vote reply.</summary>
    public TimeSpan VoteRpcTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Gets the longest time one election round can take to start: the election timeout plus the largest jitter.</summary>
    public TimeSpan Round => ElectionTimeout + MaxJitter;

    /// <summary>Builds the election options of one node.</summary>
    /// <param name="nodeId">The identifier of the node, mixed into a fixed jitter seed.</param>
    /// <returns>The election options the node registers.</returns>
    /// <exception cref="InvalidOperationException">A timeout is not positive, the jitter is negative, or the heartbeat is not below the election timeout.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The wait for a leader is not positive or above the longest the product allows.</exception>
    internal ElectionTimerOptions ToOptions(string nodeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        var options = new ElectionTimerOptions
        {
            ElectionTimeout = ElectionTimeout,
            HeartbeatInterval = HeartbeatInterval,
            MaxJitter = MaxJitter,
            VoteRpcTimeout = VoteRpcTimeout,
            LeaderWaitTimeoutOverride = LeaderWaitTimeout,
            JitterSeed = JitterSeed is { } seed ? Mix(seed, nodeId) : BinaryPrimitives.ReadUInt64LittleEndian(RandomNumberGenerator.GetBytes(sizeof(ulong))),
        };
        var valid = ElectionTimeout > TimeSpan.Zero && HeartbeatInterval > TimeSpan.Zero && HeartbeatInterval < ElectionTimeout && MaxJitter >= TimeSpan.Zero &&
                    VoteRpcTimeout > TimeSpan.Zero;
        if (!valid)
            throw new InvalidOperationException($"The election timing {this} needs positive timeouts, a non-negative jitter and a heartbeat below the election timeout.");

        ElectionTimerOptions.EnsureValidLeaderWait(options);
        return options;
    }

    /// <summary>Mixes a node identifier into a seed with FNV-1a, so one fixed seed gives every node its own stable jitter.</summary>
    /// <param name="seed">The fixed seed.</param>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>The seed of the node.</returns>
    private static ulong Mix(ulong seed, string nodeId)
    {
        var hash = 14695981039346656037UL ^ seed;
        foreach (var c in nodeId)
            hash = unchecked((hash ^ c) * 1099511628211UL);

        return hash;
    }
}
