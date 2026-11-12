using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Only election-won leadership may expose locally unflushed entries to followers.</summary>
public sealed class ReplicaLeaderLaunchTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    /// <summary>A failed local append stays private during bootstrap, but may already be replicated after an election.</summary>
    /// <param name="term">The leadership term.</param>
    /// <param name="expectedOverlap">Whether followers may receive the unflushed append.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The owner log was not opened.</exception>
    [Test]
    [Arguments(1UL, false)]
    [Arguments(2UL, true)]
    public async Task FailedAppendExposureRequiresWonElection(ulong term, bool expectedOverlap, CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(Dir, new FollowerLogOptions { FaultHooks = hooks, TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owner log is not open.");
        _ = await log.ObserveTermAsync(term, cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committer = CreateElectedCommitter(registry, "n1", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n1", "n1"));
        using var tenure = new ReplicaLeaderTenure(term, new ReplicaDriverLease());
        var (pipeline, _, _, _, _, _) = await committer.LaunchAsync(log, tenure, false, cancellationToken);
        hooks.StallNextFrameWrite();
        var appending = pipeline.AppendLocalAsync([ReplicaPipelineTestKit.Mutation(2, term)], cancellationToken).AsTask();
        try
        {
            await hooks.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            if (expectedOverlap)
            {
                await gateway.SentAsync("n2").WaitAsync(Bound, TimeProvider.System, cancellationToken);
                await gateway.SentAsync("n3").WaitAsync(Bound, TimeProvider.System, cancellationToken);
            }

            _ = await Assert.That(appending.IsCompleted).IsFalse();
            hooks.ReleaseWithFailure(new IOException("Injected leader flush failure."));
            _ = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(appending, Bound, TimeProvider.System, cancellationToken);
            _ = await pipeline.CloseAsync();
            _ = await Assert.That(gateway.Appends.IsEmpty).IsEqualTo(!expectedOverlap);
            _ = await Assert.That(pipeline.NextLogIndex).IsEqualTo(2UL);
        }
        finally
        {
            hooks.Release();
            await appending.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _ = await pipeline.CloseAsync();
        }
    }
}
