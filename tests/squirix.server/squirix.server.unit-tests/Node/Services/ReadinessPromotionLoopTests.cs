using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// The readiness service gives every leadership the election starts one verification loop, bound to its tenure, even a leadership promoted
/// before the service runs (#1184): node n1 leads group n2 by election in term 2, and both followers are down, so the group stays pending.
/// </summary>
public sealed class ReadinessPromotionLoopTests : ServerUnitTestBase
{
    /// <summary>The event id of a group whose verification is pending, reported once per loop when the outcome changes.</summary>
    private const int VerificationPendingEventId = 4004;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>How long the test watches for the report of a second loop after the first one reported.</summary>
    private static readonly TimeSpan SecondLoopWindow = TimeSpan.FromSeconds(2);

    /// <summary>A pass of each loop runs once, then waits far longer than the test: every loop reports its first outcome exactly once.</summary>
    private static readonly ReplicaReadinessOptions OnePass = new() { InitialDelay = TimeSpan.FromHours(1), MaxDelay = TimeSpan.FromHours(1) };

    /// <summary>A leadership promoted before the service starts is verified by one loop, not by one more for the led set read at start.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlyPromotionGetsOneLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-readiness-early-promotion");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = registry.TryGetLog("n2", out var log);
        _ = await log!.ObserveTermAsync(2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        await using var committers = new ReplicaGroupCommitters(
            groupId =>
            {
                var cache = new StubCache();
                return CreateElectedCommitter(registry, groupId, gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, groupId, "n1"));
            },
            new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")),
            "n1",
            Owners(),
            TimeProvider.System);
        var events = new EventRecordingLogger();
        using var service = new ReplicaGroupReadinessService(committers, events, TimeProvider.System) { Options = OnePass };

        var promoted = await committers.PromoteAsync("n2", 2UL, cancellationToken);
        await service.StartAsync(cancellationToken);
        try
        {
            await events.WhenLoggedAsync(VerificationPendingEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // A second loop would run its pass as soon as the first one leaves the commit gate, within milliseconds; the window lets it report
            // before the stop cancels it. Its absence cannot be awaited, only bounded.
            await Task.Delay(SecondLoopWindow, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That((promoted, events.Count(VerificationPendingEventId), service.ExecuteTask?.Status))
            .IsEqualTo((false, 1, TaskStatus.RanToCompletion));
    }
}
