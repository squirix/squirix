using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies async retention does not delete the active manifest during publish bursts.</summary>
public sealed class RetentionBurstTests : ServerUnitTestBase
{
    /// <summary>
    /// A cold disk read overlapping a publishing burst must not rewind the cached current state (or the roll
    /// baseline) with an older manifest it loaded from disk. The seeder writes a large manifest at index 5 so the
    /// cold <c>ReadCurrentOrDefaultAsync</c> genuinely awaits the file read and yields to the publishing burst, which
    /// installs index 20 before the resolved stale bytes would be installed into the cache. The assertion targets the
    /// final cached/allocator state (not the overlapping read's captured result, which is timing dependent): without
    /// the fix the load would overwrite the newer cached state with index 5 and rewind the allocator.
    /// </summary>
    [Fact]
    public async Task ColdReadDoesNotRewindCacheOrAllocator()
    {
        using var dir = new TempDirectory("manifest-cache-rewind");
        var options = new PersistenceOptions
        {
            DataDir = dir.Path,
            ManifestRetentionCount = 32,
        };

        using var store = new Ledger(options);
        using (var seeder = new Ledger(options))
        {
            for (var i = 1; i <= 5; i++)
            {
                var state = new State
                {
                    Format = 1,
                    CurrentJournal = i,
                    NextSequence = Convert.ToUInt64(i),
                    LastSnapshot = i == 5 ? new SnapshotRef { CreatedUtc = DateTime.UtcNow, Path = new string('x', 65000) } : null,
                };
                await seeder.WriteAsync(state, DefaultCancellationToken);
            }
        }

        var read = store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        Exception? rollError = null;
        for (var i = 6; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, OnRollFailed);

        await StoreTestSupport.WaitUntilAsync(
            store,
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            DefaultCancellationToken);

        StoreTestSupport.ThrowIfFaulted(rollError);

        _ = await read;

        var finalState = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        Assert.Equal(20, finalState.CurrentJournal);
        Assert.Equal(20, await StoreTestSupport.ReadCurrentManifestIndexAsync(dir.Path, DefaultCancellationToken));
        return;

        void OnRollFailed(Exception ex)
        {
            rollError = ex;
        }
    }

    /// <summary>Rapid publishes retain the latest manifest file and pointer.</summary>
    [Fact]
    public async Task RapidPublishBurstKeepsCurrentManifest()
    {
        using var dir = new TempDirectory("manifest-burst");
        var options = new PersistenceOptions
        {
            DataDir = dir.Path,
            ManifestRetentionCount = 2,
        };
        using var store = new Ledger(options);
        Exception? rollError = null;
        Action<Exception> onRollFailed = ex => rollError = ex;
        for (var i = 1; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, onRollFailed);

        await StoreTestSupport.WaitUntilAsync(
            store,
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            DefaultCancellationToken);

        StoreTestSupport.ThrowIfFaulted(rollError);

        Assert.True(File.Exists(NodePathKit.Combine(dir.Path, StoreTestSupport.ManifestDataFileName(20))));
    }
}
