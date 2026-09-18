using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies async retention does not delete the active manifest during publication bursts.</summary>
public sealed class RetentionBurstTests : ServerUnitTestBase
{
    /// <summary>
    /// A cold disk read overlapping a publishing burst must not rewind the cached current state (or the roll
    /// baseline) with an older manifest it loaded from the disk. The seeder writes a large manifest at index 5, so the
    /// cold <c language="csharp">ReadCurrentOrDefaultAsync</c> genuinely awaits the file read and yields to the publishing burst, which
    /// installs index 20 before the resolved stale bytes would be installed into the cache. The assertion targets the
    /// final cached/allocator state (not the overlapping read's captured result, which is timing-dependent): without
    /// the fix, the load would overwrite the newer cached state with index 5 and rewind the allocator.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="TimeoutException">Thrown if the background retention worker does not drain the burst within 30s.</exception>
    [Test]
    public async Task ColdReadDoesNotRewindCacheOrAllocator(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("manifest-cache-rewind");
        var options = new PersistenceOptions
        {
            DataDir = dir,
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
                await seeder.WriteAsync(state, cancellationToken);
            }
        }

        var read = store.ReadCurrentOrDefaultAsync(cancellationToken);
        var rollError = new StrongBox<Exception?>(null);

        for (var i = 6; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, OnRollFailed);

        // The burst drains on a background worker; under parallel CI load the default 5s poll can trip before the
        // worker finishes, so allow 30s for the journal to reach 20 (a real stall still throws).
        await store.WaitUntilValueAsync(
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            TimeSpan.FromSeconds(30),
            cancellationToken);

        Volatile.Read(ref rollError.Value).ThrowIfFaulted();

        _ = await read;

        var finalState = await store.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(finalState.CurrentJournal).IsEqualTo(20);
        _ = await Assert.That(await StoreTestSupport.ReadCurrentManifestIndexAsync(dir, cancellationToken)).IsEqualTo(20);
        return;

        void OnRollFailed(Exception ex)
        {
            _ = Interlocked.CompareExchange(ref rollError.Value, ex, null);
        }
    }

    /// <summary>Rapid publishes retain the latest manifest file and pointer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="TimeoutException">Thrown if the background retention worker does not drain the burst within 30s.</exception>
    [Test]
    public async Task RapidPublishBurstKeepsCurrentManifest(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("manifest-burst");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            ManifestRetentionCount = 2,
        };
        using var store = new Ledger(options);
        var rollError = new StrongBox<Exception?>(null);

        for (var i = 1; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, OnRollFailed);

        // The burst drains on a background worker; under parallel CI load the default 5s poll can trip before the
        // worker finishes, so allow 30s for the journal to reach 20 (a real stall still throws).
        await store.WaitUntilValueAsync(
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            TimeSpan.FromSeconds(30),
            cancellationToken);

        Volatile.Read(ref rollError.Value).ThrowIfFaulted();

        _ = await Assert.That(File.Exists(NodePathKit.Combine(dir, StoreTestSupport.ManifestDataFileName(20)))).IsTrue();
        return;

        void OnRollFailed(Exception ex)
        {
            _ = Interlocked.CompareExchange(ref rollError.Value, ex, null);
        }
    }
}
