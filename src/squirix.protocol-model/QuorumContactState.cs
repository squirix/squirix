using System;
using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>Projects an elected leader and its per-replica contact and write eligibility onto bounded logical time.</summary>
[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct QuorumContactState
{
    internal const int ContactWindow = 2;
    internal const int MaxTick = 4;
    internal const int MaxTerm = 3;

    private const int LeaderBit = 1;

    internal int ReplicaCount { get; private init; }

    internal int Term { get; private init; }

    internal int Tick { get; private init; }

    internal NodeRole Role { get; private init; }

    internal bool HasWriteQuorum => Role == NodeRole.Leader && HasMajority(Verified);

    private int LedSince { get; init; }

    /// <summary>Gets the slots whose replies arrived in the current tick.</summary>
    private int ContactsNow { get; init; }

    /// <summary>Gets the slots from the previous tick, which expire at the next tick.</summary>
    private int ContactsPrevious { get; init; }

    private int Verified { get; init; }

    /// <summary>Starts an already elected leader in term two with no verified followers.</summary>
    /// <param name="replicaCount">The configured RF.</param>
    /// <returns>The first tenure in the projection.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The RF is outside the projection.</exception>
    internal static QuorumContactState Start(int replicaCount) => replicaCount is 3 or 5
        ? new QuorumContactState { ReplicaCount = replicaCount, Term = 2, Role = NodeRole.Leader, Verified = LeaderBit }
        : throw new ArgumentOutOfRangeException(nameof(replicaCount), "The contact projection supports RF=3 and RF=5.");

    /// <summary>Starts a new tenure after an election decided outside this projection.</summary>
    /// <returns>The new tenure, including grace and empty follower contact and verification.</returns>
    /// <exception cref="InvalidOperationException">The next term is outside the projection.</exception>
    internal QuorumContactState ElectNextTerm() => Term < MaxTerm
        ? this with { Term = Term + 1, Role = NodeRole.Leader, LedSince = Tick, ContactsNow = 0, ContactsPrevious = 0, Verified = LeaderBit }
        : throw new InvalidOperationException("The contact projection has reached its term bound.");

    /// <summary>Applies a reply when it is delivered, independently of the other requests in the round.</summary>
    /// <param name="reply">A reply carrying both its original request term and the responding replica's term.</param>
    /// <param name="dropContact">Whether to drop otherwise valid probe contacts for the negative search.</param>
    /// <returns>The state after processing the reply.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The reply does not fit the projection's slot or term bounds.</exception>
    internal QuorumContactState Receive(QuorumProbeReply reply, bool dropContact = false)
    {
        if (reply.Slot <= 0 || reply.Slot >= ReplicaCount || reply.RequestTerm is < 1 or > MaxTerm || reply.ReplyTerm is < 0 or > MaxTerm)
            throw new ArgumentOutOfRangeException(nameof(reply));

        if (reply.ReplyTerm > Term)
            return Follow(reply.ReplyTerm);

        if (Role != NodeRole.Leader || reply.RequestTerm != Term || reply.Verdict == QuorumProbeVerdict.Refused)
            return this;

        var bit = 1 << reply.Slot;
        return this with
        {
            ContactsNow = dropContact ? ContactsNow : ContactsNow | bit,
            Verified = reply.Verdict == QuorumProbeVerdict.Accepted ? Verified | bit : Verified,
        };
    }

    /// <summary>Advances logical time and discards the expired contact mask.</summary>
    /// <returns>The state one tick later.</returns>
    /// <exception cref="InvalidOperationException">Time is already at the bound.</exception>
    internal QuorumContactState Advance() => Tick < MaxTick
        ? this with { Tick = Tick + 1, ContactsNow = 0, ContactsPrevious = ContactsNow }
        : throw new InvalidOperationException("The contact projection has reached its time bound.");

    internal bool HasRecentContact(int slot) => ((ContactsNow | ContactsPrevious) & (1 << slot)) != 0;

    internal bool CanCountInWriteQuorum(int slot) => (Verified & (1 << slot)) != 0;

    internal QuorumContactState CheckQuorum() =>
        Role == NodeRole.Leader && Tick - LedSince >= ContactWindow && !HasMajority(LeaderBit | ContactsNow | ContactsPrevious)
            ? Follow(Term)
            : this;

    private bool HasMajority(int votes) => VoteMask.CountGranted(votes) * 2 > ReplicaCount;

    private QuorumContactState Follow(int term) =>
        this with { Term = term, Role = NodeRole.Follower, ContactsNow = 0, ContactsPrevious = 0, Verified = 0 };
}
