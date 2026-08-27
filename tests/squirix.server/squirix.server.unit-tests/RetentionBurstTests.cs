using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
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
    /// cold <c language="csharp">ReadCurrentOrDefaultAsync</c> genuinely awaits the file read and yields to the publishing burst, which
    /// installs index 20 before the resolved stale bytes would be installed into the cache. The assertion targets the
    /// final cached/allocator state (not the overlapping read's captured result, which is timing dependent): without
    /// the fix the load would overwrite the newer cached state with index 5 and rewind the allocator.
    /// </summary>
    /// <exception cref="TimeoutException">Thrown if the background retention worker does not drain the burst within 30s.</exception>
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
        var rollError = new StrongBox<Exception?>(null);
        Action<Exception> onRollFailed = ex => _ = Interlocked.CompareExchange(ref rollError.Value, ex, null);
        for (var i = 6; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, onRollFailed);

        // The burst drains on a background worker; under parallel CI load the default 5s poll can trip before the
        // worker finishes, so allow 30s for the journal to reach 20 (a real stall still throws).
        await store.WaitUntilValueAsync(
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            TimeSpan.FromSeconds(30),
            DefaultCancellationToken);

        Volatile.Read(ref rollError.Value).ThrowIfFaulted();

        _ = await read;

        var finalState = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        Assert.Equal(20, finalState.CurrentJournal);
        Assert.Equal(20, await StoreTestSupport.ReadCurrentManifestIndexAsync(dir.Path, DefaultCancellationToken));
    }

    /// <summary>Rapid publishes retain the latest manifest file and pointer.</summary>
    /// <exception cref="TimeoutException">Thrown if the background retention worker does not drain the burst within 30s.</exception>
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
        var rollError = new StrongBox<Exception?>(null);
        Action<Exception> onRollFailed = ex => _ = Interlocked.CompareExchange(ref rollError.Value, ex, null);
        for (var i = 1; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, onRollFailed);

        // The burst drains on a background worker; under parallel CI load the default 5s poll can trip before the
        // worker finishes, so allow 30s for the journal to reach 20 (a real stall still throws).
        await store.WaitUntilValueAsync(
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            TimeSpan.FromSeconds(30),
            DefaultCancellationToken);

        Volatile.Read(ref rollError.Value).ThrowIfFaulted();

        Assert.True(File.Exists(NodePathKit.Combine(dir.Path, StoreTestSupport.ManifestDataFileName(20))));
    }
}
