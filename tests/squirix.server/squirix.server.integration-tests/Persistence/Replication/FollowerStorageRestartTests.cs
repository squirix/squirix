using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Persistence.Replication;

/// <summary>Durability of the replica-group follower log across a process restarts.</summary>
public sealed class FollowerStorageRestartTests
{
    private const string GroupId = "grp-1";

    /// <summary>A committed entry survives a process restart.</summary>
    [Fact]
    public async Task CommittedEntrySurvivesProcessRestart()
    {
        using var dir = new TempDirectory("squirix-follower-restart-committed");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(TestContext.Current.CancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), TestContext.Current.CancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, TestContext.Current.CancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(TestContext.Current.CancellationToken);
        Assert.Equal(FollowerLogReadiness.Ready, reopened.Readiness);
        Assert.Equal("a", Payload(reopened.GetCommittedEntries()));
    }

    /// <summary>An uncommitted entry remains invisible to committed reads after restart.</summary>
    [Fact]
    public async Task UncommittedEntryRemainsInvisibleAfterRestart()
    {
        using var dir = new TempDirectory("squirix-follower-restart-uncommitted");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(TestContext.Current.CancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), TestContext.Current.CancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), TestContext.Current.CancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, TestContext.Current.CancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(TestContext.Current.CancellationToken);
        _ = Assert.Single(reopened.GetCommittedEntries());
        Assert.Equal(2UL, reopened.GetStatus().LastLogIndex);
    }

    /// <summary>A crash during commit advance recovers deterministically to the advanced commit index.</summary>
    [Fact]
    public async Task CrashDuringCommitAdvanceRecoversDeterministically()
    {
        using var dir = new TempDirectory("squirix-follower-restart-crash-commit");
        var crashFaults = new CommitAdvanceFaults();

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create([GroupId]), crashFaults))
        {
            await log.OpenAsync(TestContext.Current.CancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), TestContext.Current.CancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.AdvanceCommitAsync(1UL, TestContext.Current.CancellationToken));
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1UL, reopened.GetStatus().CommitIndex);
        Assert.Equal("a", Payload(reopened.GetCommittedEntries()));
    }

    /// <summary>Corruption in the committed prefix fails readiness on restart.</summary>
    [Fact]
    public async Task CommittedPrefixCorruptionFailsReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-restart-corruption");
        var logPath = GroupStoragePaths.GetLogPath(dir, GroupId);

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(TestContext.Current.CancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), TestContext.Current.CancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, TestContext.Current.CancellationToken);
        }

        await CorruptByteAsync(logPath, 8);

        await using var reopened = OpenLog(dir);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException>(reopened.OpenAsync(TestContext.Current.CancellationToken));
        Assert.Equal(FollowerLogReadiness.Failed, reopened.Readiness);
    }

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create([GroupId]));

    private static string Payload(System.Collections.Generic.IReadOnlyList<FollowerLogEntry> entries)
    {
        var result = new System.Text.StringBuilder();
        for (var i = 0; i < entries.Count; i++)
            _ = result.Append(System.Text.Encoding.UTF8.GetString(entries[i].Payload.Span));

        return result.ToString();
    }

    private static async Task CorruptByteAsync(string path, int offset)
    {
        var bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken).ConfigureAwait(false);
        var index = offset < bytes.Length ? offset : bytes.Length - 1;
        bytes[index] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, TestContext.Current.CancellationToken).ConfigureAwait(false);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader-1",
        term,
        index - 1,
        term,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, System.Text.Encoding.UTF8.GetBytes(payload))]));

    /// <summary>Fault hooks that crash at the commit-advance boundary exactly once.</summary>
    private sealed class CommitAdvanceFaults : IFollowerLogFaultHooks
    {
        private bool _fired;

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
            if (_fired)
                return;

            _fired = true;
            throw new IOException("simulated crash during commit advance.");
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
        }
    }
}
