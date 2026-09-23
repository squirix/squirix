using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for mutating gRPC idempotency across unclean node restarts.</summary>
public sealed class RpcIdempotencyRestartTests : NodeIntegrationTestBase
{
    private const string CompactScope = "idempotency-force-kill-compact";
    private const string Scope = "idempotency-force-kill";
    private const string SetScope = "idempotency-force-kill-set";
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>After compaction and SIGKILL-style restart a retry with the same operation id must replay Added=true.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KillDuringCompactionReplaysInsert(CancellationToken cancellationToken)
    {
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-compact-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        await using var cluster = await StartClusterAsync("node-c", new IntegrationStartOptions { UsePersistence = true, ExtraScope = CompactScope }, cancellationToken);
        var node = cluster["node-c"];
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
            _ = await Assert.That(first.Added).IsTrue();
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);

        var persistence = new PersistenceOptions { DataDir = node.DataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        using var manifestStore = new Ledger(persistence);
        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(), cancellationToken);

        await using var restartCluster = await StartClusterAsync(
            "node-c",
            new IntegrationStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = CompactScope },
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
        var persistence = new PersistenceOptions { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        using var manifestStore = new Ledger(persistence);
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
