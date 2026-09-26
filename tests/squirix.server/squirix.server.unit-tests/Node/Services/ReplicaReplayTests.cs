using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A same-identity retry that the group owner answers from its idempotency state leaves the group log dense.</summary>
public sealed class ReplicaReplayTests : ServerUnitTestBase
{
    /// <summary>The replicated write kinds of the group owner.</summary>
    public enum WriteKind
    {
        /// <summary>Unconditional writing.</summary>
        Set = 0,

        /// <summary>Conditional adding.</summary>
        TryAdd = 1,

        /// <summary>Value replacement.</summary>
        Update = 2,

        /// <summary>Expiration refresh.</summary>
        Touch = 3,

        /// <summary>Removal.</summary>
        Remove = 4,

        /// <summary>Expiration removal.</summary>
        RemoveExpiration = 5,
    }

    /// <summary>
    /// A committed write retried with its operation identity is replayed without an append, and the next new write commits at the next
    /// log index without an error or a resync.
    /// </summary>
    /// <param name="kind">The kind of the replayed write.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(WriteKind.Set)]
    [Arguments(WriteKind.TryAdd)]
    [Arguments(WriteKind.Update)]
    [Arguments(WriteKind.Touch)]
    [Arguments(WriteKind.Remove)]
    [Arguments(WriteKind.RemoveExpiration)]
    public async Task ReplayThenNewWriteSucceeds(WriteKind kind, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-replay");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway());
        var operationId = NewOperationId();
        await CommitAsync(committer, kind, operationId, cancellationToken);

        await CommitAsync(committer, kind, operationId, cancellationToken);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken);

        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((2UL, 2UL));
    }

    private static Task CommitAsync(ReplicaGroupCommitter committer, WriteKind kind, string operationId, CancellationToken cancellationToken) => kind switch
    {
        WriteKind.Set => committer.CommitSetAsync(operationId, "cache", "k1", Entry("k1"), cancellationToken),
        WriteKind.TryAdd => committer.CommitTryAddAsync(operationId, "cache", "k1", Entry("k1"), cancellationToken),
        WriteKind.Update => committer.CommitUpdateAsync(operationId, "cache", "k1", "v1", cancellationToken),
        WriteKind.Touch => committer.CommitTouchAsync(operationId, "cache", "k1", TimeSpan.FromMinutes(1), cancellationToken),
        WriteKind.Remove => committer.CommitRemoveAsync(operationId, "cache", "k1", cancellationToken),
        WriteKind.RemoveExpiration => committer.CommitRemoveExpirationAsync(operationId, "cache", "k1", cancellationToken),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported write kind."),
    };
}
