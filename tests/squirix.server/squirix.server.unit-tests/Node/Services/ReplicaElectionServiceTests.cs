using System;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The election service drives every group of three or more replicas and leaves each a follower without authority when the host stops.</summary>
public sealed class ReplicaElectionServiceTests : ServerUnitTestBase
{
    /// <summary>The event id of a leader whose leader-term entry is committed.</summary>
    private const int AuthorizedEventId = 4040;

    /// <summary>The event id of a leader that gave the group up because it could not append to its own log.</summary>
    private const int AppendTimedOutEventId = 4074;

    /// <summary>The event id of a leader that gave the group up because it could not apply.</summary>
    private const int ApplyTimedOutEventId = 4073;

    /// <summary>The event id of a leader that gave the group up for want of authority.</summary>
    private const int AuthorityTimedOutEventId = 4052;

    /// <summary>The event id of a node that does not campaign because its apply is blocked.</summary>
    private const int ApplyBlockedEventId = 4076;

    /// <summary>The event id of a node that may not campaign.</summary>
    private const int DeniedEventId = 4036;

    /// <summary>The event id of a pre-vote round that found no majority.</summary>
    private const int PreVoteLostEventId = 4037;

    /// <summary>The event id of a leader that stepped down.</summary>
    private const int SteppedDownEventId = 4042;

    /// <summary>The event id of the elections stopped by the host.</summary>
    private const int StoppedEventId = 4046;

    /// <summary>The slots of the two followers of the own group, which its owner leads from slot 0.</summary>
    private const int FirstFollowerSlot = 1;

    private const int SecondFollowerSlot = 2;

    private static readonly string[] Groups = ["n1", "n2", "n3"];
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The quiet timing with an append deadline that lapses at once, so the first leader tick that finds the appends stalled twice gives up.</summary>
    private static readonly ElectionTimerOptions ImpatientAppend = new()
    {
        ElectionTimeout = TimeSpan.FromHours(1),
        MaxJitter = TimeSpan.FromHours(1),
        JitterSeed = 7UL,
        AppendTimeout = TimeSpan.FromMilliseconds(1),
    };

    /// <summary>The quiet timing with an apply deadline that lapses at once, so the first leader tick that finds the apply stalled gives up.</summary>
    private static readonly ElectionTimerOptions ImpatientApply = new()
    {
        ElectionTimeout = TimeSpan.FromHours(1),
        MaxJitter = TimeSpan.FromHours(1),
        JitterSeed = 7UL,
        ApplyTimeout = TimeSpan.FromMilliseconds(1),
    };

    /// <summary>The quiet timing with an authority deadline that lapses at once, so the first leader tick without authority gives up.</summary>
    private static readonly ElectionTimerOptions Impatient = new()
    {
        ElectionTimeout = TimeSpan.FromHours(1),
        MaxJitter = TimeSpan.FromHours(1),
        JitterSeed = 7UL,
        AuthorityTimeout = TimeSpan.FromMilliseconds(1),
    };

    /// <summary>A short election timeout, so a group that may campaign does so within the test.</summary>
    private static readonly ElectionTimerOptions Brisk = new() { ElectionTimeout = TimeSpan.FromMilliseconds(30), MaxJitter = TimeSpan.Zero, JitterSeed = 7UL };

    /// <summary>An election timeout no test run reaches, so only the own group, which needs no election, changes.</summary>
    private static readonly ElectionTimerOptions Quiet = new() { ElectionTimeout = TimeSpan.FromHours(1), MaxJitter = TimeSpan.FromHours(1), JitterSeed = 7UL };

    /// <summary>
    /// The own group at the provisional term is led at once and authorized once its leader-term entry is committed; a host stop ends every
    /// loop normally, logs one line, and revokes the authority.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnGroupLeadsUntilHostStops(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var own = registry.StateFor("n1");
        var leadership = new Leadership();
        var log = new EventRecordingLogger();
        using var service = Create(registry, 3, leadership, log);

        await service.StartAsync(cancellationToken);
        bool authorized;
        try
        {
            await log.WhenLoggedAsync(AuthorizedEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            authorized = own.HasAuthority;
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(authorized).IsTrue();
        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue().Because("A host stop ends every election loop normally.");
        _ = await Assert.That(log.Count(StoppedEventId)).IsEqualTo(1);
        _ = await Assert.That((own.HasAuthority, own.IsElectionDriven, registry.StateFor("n2").IsElectionDriven)).IsEqualTo((false, false, false));
        _ = await Assert.That(leadership.Calls).IsEqualTo("promote:n1:1");
    }

    /// <summary>
    /// The start of a graceful stop ends every loop before the hosted services stop, while the web server still drains: the own group stops
    /// heartbeating and loses its authority at once, so its followers can elect while the node finishes its requests in flight.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostStoppingRevokesAuthorityBeforeDrain(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-host-stopping");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var own = registry.StateFor("n1");
        var log = new EventRecordingLogger();
        using var hostStopping = new CancellationTokenSource();
        using var service = Create(registry, 3, new Leadership(), log, hostStopping);

        await service.StartAsync(cancellationToken);
        bool authorized;
        try
        {
            await log.WhenLoggedAsync(AuthorizedEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            authorized = own.HasAuthority;
            await hostStopping.CancelAsync();
            await service.ExecuteTask!.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(authorized).IsTrue();
        _ = await Assert.That(log.Count(StoppedEventId)).IsEqualTo(1);
        _ = await Assert.That((own.HasAuthority, own.IsElectionDriven)).IsEqualTo((false, false));
    }

    /// <summary>A leader that gets no authority within its deadline gives the group up and logs it once, as a warning, naming the group and the term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AuthorityTimeoutIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-deadline");
        await using var registry = await OpenAsync(dir, cancellationToken, Impatient);
        var leadership = new Leadership(false);
        var log = new EventRecordingLogger();
        using var service = Create(registry, 3, leadership, log);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(AuthorityTimedOutEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(log.Count(AuthorityTimedOutEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(AuthorityTimedOutEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.FindMessage(AuthorityTimedOutEventId)).Contains("term 1");
        _ = await Assert.That(log.Count(SteppedDownEventId)).IsEqualTo(0);
        _ = await Assert.That(registry.StateFor("n1").HasAuthority).IsFalse();
    }

    /// <summary>A leader with authority whose apply stands still gives the group up and logs it once, as a warning, naming the group and the term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyTimeoutIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-apply");
        await using var registry = await OpenAsync(dir, cancellationToken, ImpatientApply);
        var leadership = new Leadership { Progress = new ReplicaApplyProgress(1UL, 2UL) };
        var log = new EventRecordingLogger();
        using var service = Create(registry, 3, leadership, log);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(ApplyTimedOutEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(log.Count(ApplyTimedOutEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(ApplyTimedOutEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.FindMessage(ApplyTimedOutEventId)).Contains("term 1");
        _ = await Assert.That((log.Count(AuthorityTimedOutEventId), log.Count(SteppedDownEventId))).IsEqualTo((0, 0));
        _ = await Assert.That(registry.StateFor("n1").HasAuthority).IsFalse();
    }

    /// <summary>A leader with authority whose appends to its own log stay blocked gives the group up and logs it once, as a warning, naming the group and the term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendTimeoutIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-append");
        await using var registry = await OpenAsync(dir, cancellationToken, ImpatientAppend);
        var leadership = new Leadership { Appends = new ReplicaAppendProgress(0UL, 1UL, 0UL, false, true), Answering = registry.StateFor("n1") };
        var log = new EventRecordingLogger();
        using var service = Create(registry, 3, leadership, log);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(AppendTimedOutEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(log.Count(AppendTimedOutEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(AppendTimedOutEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.FindMessage(AppendTimedOutEventId)).Contains("term 1");
        _ = await Assert.That((log.Count(ApplyTimedOutEventId), log.Count(AuthorityTimedOutEventId), log.Count(SteppedDownEventId))).IsEqualTo((0, 0, 0));
        _ = await Assert.That(registry.StateFor("n1").HasAuthority).IsFalse();
    }

    /// <summary>An ordinary step-down of a leader logs the step-down message and never the missed-deadline warning.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OrdinaryStepDownLogsStepDown(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-stepdown");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var log = new EventRecordingLogger();
        using var service = Create(registry, 3, new Leadership(), log);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(AuthorizedEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            registry.StateFor("n1").ObserveHigherTerm(5UL);
            await log.WhenLoggedAsync(SteppedDownEventId, "group n1 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That((log.Count(SteppedDownEventId), log.Count(AuthorityTimedOutEventId))).IsEqualTo((1, 0));
    }

    /// <summary>A follower whose apply is blocked logs once per block, as a warning, that it does not campaign, and campaigns again when the block clears.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedApplyIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-blocked");
        await using var registry = await OpenAsync(dir, cancellationToken, Brisk);
        var block = registry.StateFor("n2").ApplyBlock;
        block.Block();
        var log = new EventRecordingLogger();
        using var service = Create(registry, 3, new Leadership(), log);

        await service.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(ApplyBlockedEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            block.Clear();
            await log.WhenLoggedAsync(PreVoteLostEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // A second block after the recovery is a new episode, even in the same term, and is logged again.
            block.Block();
            await WhenCountAsync(log, ApplyBlockedEventId, 2, cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(log.Count(ApplyBlockedEventId)).IsEqualTo(2);
        _ = await Assert.That(log.Find(ApplyBlockedEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.FindMessage(ApplyBlockedEventId)).Contains("group n2 ");
    }

    /// <summary>
    /// A node whose election driver armed before its apply loop looked at the group waits for it, and a committed backlog it cannot apply
    /// then blocks it: the node never makes a self-vote durable, so its term is unchanged.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedBacklogAtStartMakesNoSelfVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-startup");
        await using var registry = await OpenAsync(dir, cancellationToken, Brisk, false);
        await ReplicaApplyServiceTests.SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        _ = registry.TryGetLog("n2", out var group);
        var before = await group!.GetStatusAsync(cancellationToken);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback(static (_, _, _, _, _) => ValueTask.FromException(new JournalCapacityExceededException()));
        using var meter = new Meter("test");
        var appliers = ReplicaApplyServiceTests.CreateAppliers(registry, cache.Instance(), meter);
        var log = new EventRecordingLogger();
        using var election = Create(registry, 3, new Leadership(), log);
        using var apply = new ReplicaApplyService(
            registry,
            appliers,
            ReplicaApplyServiceTests.LeadOwnGroup(registry, appliers),
            ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            log,
            new FakeTimeProvider());

        await election.StartAsync(cancellationToken);
        try
        {
            await log.WhenLoggedAsync(DeniedEventId, "group n2 may not campaign").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await apply.StartAsync(cancellationToken);
            try
            {
                await log.WhenLoggedAsync(ApplyBlockedEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            }
            finally
            {
                await apply.StopAsync(cancellationToken);
            }
        }
        finally
        {
            await election.StopAsync(cancellationToken);
        }

        var after = await group.GetStatusAsync(cancellationToken);
        _ = await Assert.That((after.CurrentTerm, after.VotedFor)).IsEqualTo((before.CurrentTerm, before.VotedFor));
    }

    /// <summary>Groups of two replicas run no election loop: their owner leads them statically, and nothing is promoted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TwoReplicasRunNoLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-service-rf2");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var leadership = new Leadership();
        using var service = Create(registry, 2, leadership, new EventRecordingLogger());

        await service.StartAsync(cancellationToken);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await service.StopAsync(cancellationToken);

        _ = await Assert.That((leadership.Calls, registry.StateFor("n1").IsElectionDriven)).IsEqualTo((string.Empty, false));
    }

    private static async Task WhenCountAsync(EventRecordingLogger log, int eventId, int count, CancellationToken cancellationToken)
    {
        while (log.Count(eventId) < count)
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, cancellationToken);
    }

    private static ReplicaElectionService Create(ReplicaGroupRegistry registry, int replicaCount, Leadership leadership, EventRecordingLogger log, CancellationTokenSource? hostStopping = null) => new(
        registry,
        new FixedLocator(replicaCount),
        new NoVotes(),
        leadership,
        (Fingerprint, 1UL, "n1"),
        log)
    {
        HostStopping = hostStopping?.Token ?? CancellationToken.None,
    };

    private static async Task<ReplicaGroupRegistry> OpenAsync(string dir, CancellationToken cancellationToken, ElectionTimerOptions? election = null, bool assessed = true)
    {
        await SeededLogs.SeedGroupsAsync(dir, Groups, cancellationToken);
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System, Election = election ?? Quiet };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        // The apply loop is not part of these tests: it would have looked at every group before the drivers campaign.
        for (var i = 0; assessed && i < Groups.Length; i++)
            registry.StateFor(Groups[i]).ApplyBlock.Assess();

        return registry;
    }

    /// <summary>Voters that never answer, so no group campaigns to any effect.</summary>
    private sealed class NoVotes : IReplicaVoteGateway
    {
        public Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            Task.FromException<FollowerLogVoteResult>(new IOException("voter unreachable"));

        public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
            Task.FromException<FollowerLogVoteResult>(new IOException("voter unreachable"));
    }

    /// <summary>A leadership whose every promotion is committed at once unless it is told otherwise, recording the promoted terms.</summary>
    private sealed class Leadership : IReplicaLeadership
    {
        private readonly ConcurrentQueue<string> _calls = new();
        private readonly bool _commits;

        internal Leadership(bool commits = true)
        {
            _commits = commits;
        }

        internal string Calls => string.Join(',', _calls);

        internal ReplicaAppendProgress Appends { get; init; }

        internal ReplicaGroupState? Answering { get; init; }

        internal ReplicaApplyProgress Progress { get; init; }

        public Task HeartbeatAsync(string groupId, CancellationToken cancellationToken)
        {
            if (Answering is { } state)
            {
                var reply = new FollowerLogAppendResult(true, string.Empty, state.Term, 0UL);
                state.RecordFollowerReply(FirstFollowerSlot, in reply);
                state.RecordFollowerReply(SecondFollowerSlot, in reply);
            }

            return Task.CompletedTask;
        }

        public ReplicaApplyProgress ApplyProgress(string groupId) => Progress;

        public ReplicaAppendProgress AppendProgress(string groupId) => Appends;

        public Task<bool> PromoteAsync(string groupId, ulong term, CancellationToken cancellationToken)
        {
            _calls.Enqueue($"promote:{groupId}:{term}");
            return Task.FromResult(_commits);
        }

        public Task<bool> RetireAsync(string groupId, CancellationToken cancellationToken)
        {
            _calls.Enqueue($"retire:{groupId}");
            return Task.FromResult(true);
        }
    }

    /// <summary>Places the owner of a group first and the other nodes of n1, n2, n3 after it, up to the replica count.</summary>
    private sealed class FixedLocator : IReplicaGroupLocator
    {
        internal FixedLocator(int replicaCount)
        {
            ReplicaCount = replicaCount;
        }

        public int ReplicaCount { get; }

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            var first = Array.IndexOf(Groups, originalOwnerNodeId);
            for (var i = 0; i < destination.Length; i++)
                destination[i] = Groups[(first + i) % Groups.Length];
        }
    }
}
