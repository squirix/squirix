using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The service of the first start repeats an iteration with a backoff or on a peer's call until the copy is complete, and logs each outcome once.</summary>
[Immutable]
public sealed class ReplicaGroupBootstrapServiceTests : ServerUnitTestBase
{
    /// <summary>The event id of a group waiting for a member.</summary>
    private const int WaitingEventId = 4053;

    /// <summary>The event id of a group seeded by its first start.</summary>
    private const int CompletedEventId = 4056;

    /// <summary>The event id of a first start stopped by the host.</summary>
    private const int StoppedEventId = 4061;

    private const string Group = "n1";

    private static readonly StartedLifetime AlreadyStarted = new(new CancellationToken(true));
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);
    private static readonly string[] Members = ["n1", "n2", "n3"];

    /// <summary>The committers of a node that leads no group, which the service has nothing to wake in.</summary>
    private static readonly ReplicaGroupCommitters NoLedGroups = new([], Group, new INodeLocatorCreateExpectations().Instance(), TimeProvider.System);

    /// <summary>The service ends normally once the copy is complete, after logging the start once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServiceEndsOnceCopyIsComplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-ends");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        var events = new EventRecordingLogger();
        using var service = Create(log, gateway, events, (new FakeTimeProvider(), new ReplicaPeerContact(), AlreadyStarted));

        await service.StartAsync(cancellationToken);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(events.Count(CompletedEventId)).IsEqualTo(1);
        _ = await Assert.That(gateway.Calls).IsEqualTo("status:n2,status:n3,offer:n2,offer:n3");
        await service.StopAsync(cancellationToken);
    }

    /// <summary>With no wake the service retries on its own backoff, which starts at the initial delay, doubles, and stops at the maximum.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServiceBacksOffOnTheClock(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-backoff");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Down("n3");
        var clock = new TimerSignalClock();
        using var service = Create(log, gateway, new EventRecordingLogger(), (clock, new ReplicaPeerContact(), AlreadyStarted));
        var oneMillisecond = TimeSpan.FromMilliseconds(1);

        await service.StartAsync(cancellationToken);
        await RoundAsync(gateway, clock, cancellationToken);
        var delay = ReplicaGroupBootstrapService.InitialDelay;
        for (var round = 1; round <= 7; round++)
        {
            _ = await Assert.That(gateway.Calls).IsEqualTo(Expected(round));
            clock.Advance(delay - oneMillisecond);
            _ = await Assert.That(gateway.Calls).IsEqualTo(Expected(round));
            clock.Advance(oneMillisecond);
            await RoundAsync(gateway, clock, cancellationToken);
            delay = TimeSpan.FromTicks(Math.Min(ReplicaGroupBootstrapService.MaxDelay.Ticks, delay.Ticks * 2));
        }

        _ = await Assert.That(delay).IsEqualTo(ReplicaGroupBootstrapService.MaxDelay);
        await service.StopAsync(cancellationToken);
        _ = await Assert.That(gateway.Offers).IsEqualTo(0);
    }

    /// <summary>
    /// The call of the member the service waits for runs an iteration at once, with no clock advance; when that iteration found the same state, the
    /// next call of the member waits for the initial delay since the iteration started, and the copy completes once every member is ready.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallOfAwaitedMemberWakesTheLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-wake");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Down("n3");
        var clock = new TimerSignalClock();
        var contact = new ReplicaPeerContact();
        using var service = Create(log, gateway, new EventRecordingLogger(), (clock, contact, AlreadyStarted));
        await service.StartAsync(cancellationToken);
        await RoundAsync(gateway, clock, cancellationToken);

        contact.Notify("n3");
        await RoundAsync(gateway, clock, cancellationToken);

        _ = await Assert.That(gateway.Calls).IsEqualTo(Expected(2));
        _ = await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);

        gateway.Report("n3", Unseeded());
        contact.Notify("n3");
        _ = await clock.TimerCreated.WaitAsync(HangGuard, cancellationToken);

        _ = await Assert.That(gateway.Calls).IsEqualTo(Expected(2));

        clock.Advance(ReplicaGroupBootstrapService.InitialDelay);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
        await service.StopAsync(cancellationToken);
    }

    /// <summary>A wake after the copy is complete does nothing: the service has ended and nobody is called again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WakeAfterCompletionDoesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-late-wake");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        var contact = new ReplicaPeerContact();
        using var service = Create(log, gateway, new EventRecordingLogger(), (new FakeTimeProvider(), contact, AlreadyStarted));
        await service.StartAsync(cancellationToken);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var before = gateway.Calls;

        contact.Notify("n2");
        contact.Notify("n3");

        _ = await Assert.That(gateway.Calls).IsEqualTo(before);
        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue();
        await service.StopAsync(cancellationToken);
    }

    /// <summary>The first iteration does not run before the application has started, and runs once it has.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstIterationWaitsForStarted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-started");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        using var started = new CancellationTokenSource();
        var lifetime = new StartedLifetime(started.Token);
        using var service = Create(log, gateway, new EventRecordingLogger(), (new FakeTimeProvider(), new ReplicaPeerContact(), lifetime));

        await service.StartAsync(cancellationToken);
        _ = await Assert.That(gateway.Calls).IsEqualTo(string.Empty);
        _ = await Assert.That(service.ExecuteTask?.IsCompleted).IsFalse();
        await started.CancelAsync();
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(gateway.Calls).IsEqualTo("status:n2,status:n3,offer:n2,offer:n3");
        await service.StopAsync(cancellationToken);
    }

    /// <summary>An outcome that does not change is logged once; the change to the completed copy is logged once more, and a host stop logs one line.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnchangedOutcomeIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-log");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Down("n3");
        var clock = new TimerSignalClock();
        var events = new EventRecordingLogger();
        using var service = Create(log, gateway, events, (clock, new ReplicaPeerContact(), AlreadyStarted));

        await service.StartAsync(cancellationToken);
        await RoundAsync(gateway, clock, cancellationToken);
        clock.Advance(ReplicaGroupBootstrapService.InitialDelay);
        await RoundAsync(gateway, clock, cancellationToken);
        clock.Advance(ReplicaGroupBootstrapService.InitialDelay * 2);
        await RoundAsync(gateway, clock, cancellationToken);

        _ = await Assert.That(events.Count(WaitingEventId)).IsEqualTo(1);
        _ = await Assert.That(events.FindMessage(WaitingEventId)).Contains("node n3");

        gateway.Report("n3", Unseeded());
        clock.Advance(ReplicaGroupBootstrapService.InitialDelay * 4);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((events.Count(WaitingEventId), events.Count(CompletedEventId), events.Count(StoppedEventId))).IsEqualTo((1, 1, 0));
        await service.StopAsync(cancellationToken);
    }

    /// <summary>A host stop while the copy waits for a member ends the service normally with one line.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostStopEndsServiceWithOneLine(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-stop");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Down("n2");
        gateway.Down("n3");
        var clock = new TimerSignalClock();
        var events = new EventRecordingLogger();
        using var service = Create(log, gateway, events, (clock, new ReplicaPeerContact(), AlreadyStarted));
        await service.StartAsync(cancellationToken);
        await RoundAsync(gateway, clock, cancellationToken);

        await service.StopAsync(cancellationToken);

        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(events.Count(StoppedEventId)).IsEqualTo(1);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Unseeded);
    }

    /// <summary>Seeding the own copy wakes the readiness loop of the group the node leads at once, without the clock advancing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SeedingWakesReadinessLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-wake");
        var clock = new FakeTimeProvider();
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        await using var registry = new ReplicaGroupRegistry(dir, [Group], 3, Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
        await registry.OpenAsync(cancellationToken);
        _ = registry.TryGetLog(Group, out var registered);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        await using var own = LedGroupsTestKit.CreateGroupCommitter(registry, Group, new ReplicaOwnerTestKit.ScriptedGateway(), new ReplicaOwnerTestKit.StubCache(), clock, (clock, null));
        await using var committers = LedGroupsTestKit.LeadOwn(own);
        var bootstrap = new ReplicaGroupBootstrap(registered!, gateway, Members, new ReplicaRpcHeader(Group, Fingerprint, 1UL, 0UL, string.Empty, Group), TimeSpan.FromHours(1));
        using var service = new ReplicaGroupBootstrapService(bootstrap, (new ReplicaPeerContact(), AlreadyStarted, committers, new ReplicaApplySignal()), new EventRecordingLogger(), clock);

        await service.StartAsync(cancellationToken);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var woken = await own.Probe.Repairs.WaitAsync(TimeSpan.FromHours(1), clock, cancellationToken);

        _ = await Assert.That((await registered!.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(woken).IsEqualTo(TimedWaitResult.Signaled);
        await service.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Seeding the own copy notifies the apply signal of the group at once, so its apply loop assesses the copy without waiting out its fallback
    /// interval; the wait here has a delay of an hour on a clock that never advances, so only the notification can complete it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SeedingNotifiesApplySignal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-bootstrap-service-apply-signal");
        await using var log = await OpenAsync(dir, cancellationToken);
        var gateway = new ScriptedBootstrapGateway();
        gateway.Report("n2", Unseeded());
        gateway.Report("n3", Unseeded());
        var clock = new FakeTimeProvider();
        var signal = new ReplicaApplySignal();
        var bootstrap = new ReplicaGroupBootstrap(log, gateway, Members, new ReplicaRpcHeader(Group, Fingerprint, 1UL, 0UL, string.Empty, Group), TimeSpan.FromHours(1));
        using var service = new ReplicaGroupBootstrapService(bootstrap, (new ReplicaPeerContact(), AlreadyStarted, NoLedGroups, signal), new EventRecordingLogger(), clock);

        await service.StartAsync(cancellationToken);
        await (service.ExecuteTask ?? Task.CompletedTask).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var woken = await signal.WaitAsync(TimeSpan.FromHours(1), clock, cancellationToken);

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(woken).IsEqualTo(TimedWaitResult.Signaled);
        await service.StopAsync(cancellationToken);
    }

    /// <summary>Builds the calls of a number of iterations that found the third member down.</summary>
    /// <param name="rounds">The number of iterations.</param>
    /// <returns>The status calls in order.</returns>
    private static string Expected(int rounds)
    {
        var parts = new string[rounds];
        Array.Fill(parts, "status:n2,status:n3");
        return string.Join(',', parts);
    }

    /// <summary>Waits for the two status calls of one iteration and for the wait timer the service arms after it.</summary>
    /// <param name="gateway">The scripted members.</param>
    /// <param name="clock">The clock that signals each timer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the service waits for the next iteration.</returns>
    private static async Task RoundAsync(ScriptedBootstrapGateway gateway, TimerSignalClock clock, CancellationToken cancellationToken)
    {
        await gateway.WaitForStatusCallsAsync(2, cancellationToken);
        _ = await clock.TimerCreated.WaitAsync(HangGuard, cancellationToken);
    }

    private static ReplicaGroupBootstrapService Create(
        FollowerLog log,
        ScriptedBootstrapGateway gateway,
        EventRecordingLogger events,
        (TimeProvider Clock, ReplicaPeerContact Contact, StartedLifetime Lifetime) schedule)
    {
        var bootstrap = new ReplicaGroupBootstrap(log, gateway, Members, new ReplicaRpcHeader(Group, Fingerprint, 1UL, 0UL, string.Empty, Group), TimeSpan.FromHours(1));
        return new ReplicaGroupBootstrapService(bootstrap, (schedule.Contact, schedule.Lifetime, NoLedGroups, new ReplicaApplySignal()), events, schedule.Clock);
    }

    private static ReplicaMemberStatus Unseeded() => new(GroupMemberState.Unseeded, true, Fingerprint, 1UL, string.Empty);

    private static async Task<FollowerLog> OpenAsync(string dir, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, Group, GroupComposition.Create(Group), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenAsync(cancellationToken);
        return log;
    }

    /// <summary>A fake clock that signals each timer created on it, so a test advances only once a wait is armed.</summary>
    [ThreadSafe]
    private sealed class TimerSignalClock : FakeTimeProvider
    {
        internal TimerSignalClock()
            : base(DateTimeOffset.UnixEpoch)
        {
        }

        internal SemaphoreSlim TimerCreated { get; } = new(0);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = base.CreateTimer(callback, state, dueTime, period);
            _ = TimerCreated.Release();
            return timer;
        }
    }
}
