using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
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

    /// <summary>The event id of a leader that gave the group up for want of authority.</summary>
    private const int AuthorityTimedOutEventId = 4052;

    /// <summary>The event id of a leader that stepped down.</summary>
    private const int SteppedDownEventId = 4042;

    /// <summary>The event id of the elections stopped by the host.</summary>
    private const int StoppedEventId = 4046;

    private static readonly string[] Groups = ["n1", "n2", "n3"];
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The quiet timing with an authority deadline that lapses at once, so the first leader tick without authority gives up.</summary>
    private static readonly ElectionTimerOptions Impatient = new()
    {
        ElectionTimeout = TimeSpan.FromHours(1),
        MaxJitter = TimeSpan.FromHours(1),
        JitterSeed = 7UL,
        AuthorityTimeout = TimeSpan.FromMilliseconds(1),
    };

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

    private static async Task<ReplicaGroupRegistry> OpenAsync(string dir, CancellationToken cancellationToken, ElectionTimerOptions? election = null)
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

        public Task HeartbeatAsync(string groupId, CancellationToken cancellationToken) => Task.CompletedTask;

        public ReplicaApplyProgress ApplyProgress(string groupId) => default;

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
