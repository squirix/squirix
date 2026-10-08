using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The leader committer records the outcome of an entry its catch-up applies once the outcomes of its group log are rebuilt.</summary>
public sealed class LeaderOutcomeTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>
    /// An entry committed after its coordinator retired, with its pin still unresolved, is applied by the catch-up of the next start, which
    /// resolves the pin with the outcome the entry carries: a retry replays it instead of reporting an unknown outcome.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The owned group log is not open, or the prepared record does not decode.</exception>
    [Test]
    public async Task CatchUpResolvesRetiredPin(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-outcome-pin");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var cache = new StubCache();
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "k0", Entry("k0"), cancellationToken);
        var log = registry.TryGetLog("n1", out var owned) ? owned : throw new InvalidOperationException("The owned group log is not open.");
        var rebuilt = log.Idempotency.OutcomesRebuilt;

        // The late commit of a retired coordinator: the add is pinned, appended and committed at index 2, but never applied here.
        var operationId = NewOperationId();
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        var add = await factory.PrepareTryAddAsync(operationId, CacheName, "k1", Entry("k1"), 2UL, cancellationToken);
        var record = ReplicaLogCodec.Decode(add.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record does not decode."));
        _ = log.Idempotency.Reserve(record.OperationScope, record.OperationId, record.OperationFingerprint.Span, GroupRecordKind.UserMutation, 2UL, 1UL);
        FollowerLogEntry[] entry = [new(2UL, 1UL, add.CanonicalPayload)];
        var appended = await log.AppendAsync(new FollowerLogAppendRequest("n1", 1UL, 1UL, 1UL, 2UL, entry), cancellationToken);
        committer.DropStartedState();

        var replayed = await committer.CommitTryAddAsync(operationId, CacheName, "k1", Entry("k1"), cancellationToken);

        _ = await Assert.That((rebuilt, appended.Success)).IsEqualTo((true, true));
        _ = await Assert.That(replayed).IsTrue();
        _ = await Assert.That(log.Idempotency.Lookup(record.OperationScope, record.OperationId, record.OperationFingerprint.Span, out _)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        await SequenceAssert.EqualAsync(["k0", "k1"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }
}
