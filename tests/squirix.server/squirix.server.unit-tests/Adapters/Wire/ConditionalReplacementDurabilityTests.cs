using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>Checks the owner-side conditional replacement at the journal and memory boundary.</summary>
public sealed class ConditionalReplacementDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>An accepted journal frame that cannot be written never applies the replacement in memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task FailedAppendLeavesValueAndDeadline(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var initial = FusedWriteHarness.Set(string.Empty, "initial", ttl: TimeSpan.FromMinutes(1));
        _ = await harness.Adapter.Mutations.SetEntryAsync(initial, cancellationToken);
        var before = await harness.Cache.GetEntryAsync(initial.CacheName, initial.Key, null, cancellationToken);
        journal.Writer.Write.Arm();
        var replacement = FusedWriteHarness.Set(string.Empty, "replacement", ttl: TimeSpan.FromMinutes(2));
        var writing = harness.Adapter.Mutations.ReplaceEntryIfPresentAsync(replacement, cancellationToken).AsTask();
        try
        {
            await journal.Writer.Write.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            journal.Writer.Write.ReleaseWithFailure(new IOException("journal write failed"));
            var error = await NodeAsyncAssert.ThrowsAsync<SquirixException, UpdateAsyncResponse>(
                new ValueTask<UpdateAsyncResponse>(writing),
                TimeSpan.FromSeconds(10),
                TimeProvider.System,
                cancellationToken);
            _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        }
        finally
        {
            journal.Writer.Write.Release();
        }

        var after = await harness.Cache.GetEntryAsync(initial.CacheName, initial.Key, null, cancellationToken);
        _ = await Assert.That(after!.ExpiresUtc).IsEqualTo(before!.ExpiresUtc);
        _ = await Assert.That(after.Value.Memory.Span.SequenceEqual(before.Value.Memory.Span)).IsTrue();
        _ = await Assert.That(harness.ReadFrames(cancellationToken).Count).IsEqualTo(1);
    }

    /// <summary>A native set waits for a same-key conditional replacement to become durable and apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task NativeSetWaitsForReplacement(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var initial = FusedWriteHarness.Set(string.Empty, "initial", ttl: TimeSpan.FromMinutes(1));
        _ = await harness.Adapter.Mutations.SetEntryAsync(initial, cancellationToken);
        journal.Writer.Flush.Arm();

        var replacement = FusedWriteHarness.Set(string.Empty, "replacement", ttl: TimeSpan.FromMinutes(2));
        var replacing = harness.Adapter.Mutations.ReplaceEntryIfPresentAsync(replacement, cancellationToken).AsTask();
        try
        {
            await journal.Writer.Flush.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            var native = FusedWriteHarness.Set(string.Empty, "native", ttl: TimeSpan.FromMinutes(3));
            var competing = harness.Adapter.Mutations.SetEntryAsync(native, cancellationToken).AsTask();
            _ = await Assert.That(competing.IsCompleted).IsFalse();
            journal.Writer.Flush.Release();

            _ = await Assert.That((await replacing).Updated).IsTrue();
            _ = await competing;
            var stored = await harness.Cache.GetEntryAsync(initial.CacheName, initial.Key, null, cancellationToken);
            _ = await Assert.That(stored!.ExpiresUtc).IsEqualTo(harness.Clock.GetUtcNow().UtcDateTime.AddMinutes(3));
            _ = await Assert.That(stored.Value.Memory.Span.SequenceEqual(native.Entry!.Value.Value!.JsonUtf8.Span)).IsTrue();
            _ = await Assert.That(harness.ReadFrames(cancellationToken).Count).IsEqualTo(3);
        }
        finally
        {
            journal.Writer.Flush.Release();
        }
    }
}
