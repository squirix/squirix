using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Verifies async retention does not delete the active manifest during publication bursts.</summary>
public sealed class RetentionBurstTests : ServerUnitTestBase
{
    /// <summary>Rapid publishes leave the latest manifest in the slots.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="TimeoutException">Thrown if the background retention worker does not drain the burst within 30s.</exception>
    [Test]
    public async Task RapidPublishBurstKeepsCurrentManifest(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("manifest-burst");
        var options = new PersistenceOptions
        {
            DataDir = dir,
        };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var rollError = new StrongBox<Exception?>(null);

        for (var i = 1; i <= 20; i++)
            store.EnqueueRoll(i, Convert.ToUInt64(i), static () => { }, OnRollFailed);

        // The burst drains on a background worker; under parallel CI load the default 5s poll can trip before the
        // worker finishes, so allow 30s for the journal to reach 20 (a real stall still throws).
        await store.WaitUntilValueAsync(
            static async (s, ct) => (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 20,
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        Volatile.Read(ref rollError.Value).ThrowIfFaulted();

        _ = await Assert.That(StoreTestSupport.ReadCurrentSelection(dir).State.CurrentJournal).IsEqualTo(20);
        return;

        void OnRollFailed(Exception ex)
        {
            _ = Interlocked.CompareExchange(ref rollError.Value, ex, null);
        }
    }
}
