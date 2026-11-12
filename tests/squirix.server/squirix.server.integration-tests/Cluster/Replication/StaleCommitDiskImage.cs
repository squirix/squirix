using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Models loss of commit metadata while retaining the durable WAL and election state.</summary>
internal static class StaleCommitDiskImage
{
    internal static async Task AssertCheckpointAsync(ITestNodeHost host, string group, ulong term, ulong index, CancellationToken token)
    {
        var saved = await GroupMetadataFile.ReadAsync(GroupStoragePaths.GetMetadataPath(host.DataDir, group), token);
        _ = await Assert.That(saved.CurrentTerm).IsEqualTo(term);
        _ = await Assert.That(saved.CommitIndex).IsGreaterThanOrEqualTo(index);
    }

    internal static async Task AssertStaleAsync(string path, ulong index, CancellationToken token) =>
        _ = await Assert.That((await GroupMetadataFile.ReadAsync(path, token)).CommitIndex).IsLessThan(index);

    internal static async Task<Checkpoint[]> CaptureAsync(TestCluster<IntegrationStartOptions> cluster, string[] nodes, string group, ulong term, ulong index, CancellationToken token)
    {
        var checkpoints = new Checkpoint[nodes.Length];
        for (var i = 0; i < nodes.Length; i++)
        {
            var path = GroupStoragePaths.GetMetadataPath(cluster[nodes[i]].DataDir, group);
            var bytes = await GroupMetadataFile.ReadBytesAsync(path, token);
            var read = GroupMetadataSlots.Select(bytes);
            _ = await Assert.That(read.Status).IsEqualTo(GroupMetadataReadStatus.Valid);
            _ = await Assert.That(read.Metadata.CurrentTerm).IsEqualTo(term);
            _ = await Assert.That(read.Metadata.CommitIndex).IsLessThan(index);
            checkpoints[i] = new Checkpoint(path, bytes, read.Metadata);
        }

        return checkpoints;
    }

    internal static Task CompactAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost host, string group, CancellationToken token) =>
        ledger.UntilValueAsync(
            (Committer: host.GetRequiredService<ReplicaGroupCommitters>().For(group), Journal: host.GetRequiredService<IJournalCoordinator>()),
            static async (state, ct) =>
            {
                await state.Committer.FlushAppliedAsync(state.Journal, ct);
                var outcome = await state.Committer.CompactOwnedLogAsync(new ReplicaLogCompactionPolicy(long.MaxValue, 1), state.Journal, ct);
                return outcome == ReplicaLogCompactionOutcome.Compacted;
            },
            "the deferred index is checkpointed and the WAL is compacted",
            token);

    internal static async Task CrashAndRestoreAsync(TestCluster<IntegrationStartOptions> cluster, string[] nodes, Checkpoint[] checkpoints, ulong index, CancellationToken token)
    {
        await Task.WhenAll(Array.ConvertAll(nodes, node => cluster[node].AbruptShutdownAsync().AsTask()));
        foreach (var node in nodes)
            await cluster.StopNodeAsync(node);
        foreach (var (path, bytes, old) in checkpoints)
        {
            var latest = await GroupMetadataFile.ReadAsync(path, token);
            _ = await Assert.That(latest.TopologyFingerprint.Span.SequenceEqual(old.TopologyFingerprint.Span)).IsTrue();
            _ = await Assert.That(latest with { CommitIndex = old.CommitIndex, LastAppliedIndex = old.LastAppliedIndex, TopologyFingerprint = old.TopologyFingerprint }).IsEqualTo(old);
            await File.WriteAllBytesAsync(path, bytes, token);
            var restored = await GroupMetadataFile.ReadAsync(path, token);
            _ = await Assert.That(restored.CommitIndex).IsLessThan(index);
            _ = await Assert.That(restored.LastAppliedIndex).IsLessThanOrEqualTo(restored.CommitIndex);
        }
    }

    internal sealed record Checkpoint(string Path, byte[] Bytes, GroupLogMetadata Metadata);
}
