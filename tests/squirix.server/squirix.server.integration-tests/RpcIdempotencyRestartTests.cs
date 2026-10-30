using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for mutating gRPC idempotency across unclean node restarts.</summary>
public sealed class RpcIdempotencyRestartTests : NodeIntegrationTestBase
{
    private const string SnapshotScope = "idempotency-force-kill-snapshot";
    private const string Scope = "idempotency-force-kill";
    private const string SetScope = "idempotency-force-kill-set";
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>After a snapshot and a SIGKILL-style restart a retry with the same operation id must replay Added=true.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KillAfterSnapshotReplaysInsert(CancellationToken cancellationToken)
    {
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-snapshot-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        await using var cluster = await StartClusterAsync("node-c", new IntegrationStartOptions { UsePersistence = true, ExtraScope = SnapshotScope }, cancellationToken);
        var node = cluster["node-c"];
        var snapshot = node.GetRequiredService<Coordinator>();
        var published = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        snapshot.SnapshotCompleted += (_, _) => published.TrySetResult();
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(first.Added).IsTrue();
        }

        // The periodic trigger may publish first; either snapshot satisfies the test.
        _ = await snapshot.SnapshotAsync(node.GetRequiredService<IJournalCoordinator>(), cancellationToken);
        await published.Task.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);

        await using var restartCluster = await StartClusterAsync(
            "node-c",
            new IntegrationStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = SnapshotScope },
            cancellationToken);
        using (var channel = CreateGrpcChannel(restartCluster["node-c"].Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(retry.Added).IsTrue();
        }
    }

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay Added=true even though the key was recovered from the journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KillRestartReplaysInsertIdempotency(CancellationToken cancellationToken)
    {
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        await using var cluster = await StartClusterAsync("node-a", new IntegrationStartOptions { UsePersistence = true, ExtraScope = Scope }, cancellationToken);
        var node = cluster["node-a"];
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(first.Added).IsTrue();
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
        await JournalHasPutAndIdempotencyRecordsAsync(node.DataDir);

        await using var restartCluster = await StartClusterAsync(
            "node-a",
            new IntegrationStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = Scope },
            cancellationToken);
        using (var channel = CreateGrpcChannel(restartCluster["node-a"].Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(retry.Added).IsTrue();

            var get = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "force-kill-idempotency" }, cancellationToken: cancellationToken);
            _ = await Assert.That(get.Found).IsTrue();
        }
    }

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay the original Set response.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KillRestartReplaysSetIdempotency(CancellationToken cancellationToken)
    {
        var request = new SetEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-set-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "set-value", Version = 1 }.MapToProto(),
        };

        await using var cluster = await StartClusterAsync("node-b", new IntegrationStartOptions { UsePersistence = true, ExtraScope = SetScope }, cancellationToken);
        var node = cluster["node-b"];
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.SetEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(first).IsNotNull();
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);

        await using var restartCluster = await StartClusterAsync(
            "node-b",
            new IntegrationStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = SetScope },
            cancellationToken);
        using (var channel = CreateGrpcChannel(restartCluster["node-b"].Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.SetEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(retry).IsNotNull();

            var get = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "force-kill-set-idempotency" }, cancellationToken: cancellationToken);
            _ = await Assert.That(get.Found).IsTrue();
        }
    }

    private static async Task JournalHasPutAndIdempotencyRecordsAsync(string dataDir)
    {
        var persistence = new PersistenceOptions { DataDir = dataDir, JournalMaxSegmentMb = 16 };
        using var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None).ConfigureAwait(false);
        var sawPut = false;
        var sawIdempotency = false;
        using var records = JournalReadPath.ReadAll(dataDir, manifest.CurrentJournal, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is JournalOperationKind.Put)
                sawPut = true;
            if (record.Operation is JournalOperationKind.IdempotencyOutcome)
                sawIdempotency = true;
        }

        _ = await Assert.That(sawPut).IsTrue();
        _ = await Assert.That(sawIdempotency).IsTrue();
    }
}
