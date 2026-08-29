using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Concurrency and lifecycle tests for <see cref="JournalCompactionController" />.
/// </summary>
[Immutable]
public sealed class JournalCompactionControllerTests : IsolatedStorageTestBase
{
    /// <summary>Double dispose does not throw.</summary>
    [Fact]
    [SuppressMessage("Major Code Smell", "S2699:Tests should include assertions", Justification = "This lifecycle test asserts that the second Dispose call does not throw.")]
    [SuppressMessage("ReSharper", "DisposeOnUsingVariable", Justification = "Dispose must be called two times")]
    public async Task DisposeIsIdempotent()
    {
        var opt = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 1000 };
        using var manifestStore = new Ledger(opt);
        await using var journal = JournalCoordinatorFactory.Create(opt, new State(), manifestStore, new AsyncManualResetEvent(true));
        using var controller = new JournalCompactionController(opt, manifestStore, StoreFactory.CreateReader(opt), journal, NullLogger<JournalCompactionController>.Instance);
        controller.Dispose();
    }

    /// <summary>
    /// When the controller compaction mutex is already held, <see cref="JournalCompactionController.TryTriggerAsync" /> returns false without waiting.
    /// </summary>
    [Fact]
    public async Task TriggerNowFalseWhenMutexUnavailableAsync()
    {
        var opt = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 16,
            FlushInterval = 1000,
        };

        using var manifestStore = new Ledger(opt);
        await using var journal = JournalCoordinatorFactory.Create(opt, new State(), manifestStore, new AsyncManualResetEvent(true));
        await journal.AppendPutAsync(CacheKey.Default("gate"), JournalEntryPayloadKit.EncodePut("x"), DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        using var controller = new JournalCompactionController(opt, manifestStore, StoreFactory.CreateReader(opt), journal, NullLogger<JournalCompactionController>.Instance);

        var firstTrigger = controller.TryTriggerAsync(DefaultCancellationToken);
        var secondTrigger = controller.TryTriggerAsync(DefaultCancellationToken);
        var firstResult = await firstTrigger;
        var secondResult = await secondTrigger;

        Assert.True(firstResult ^ secondResult);
        Assert.True(await controller.TryTriggerAsync(DefaultCancellationToken));
    }

    /// <summary>Disposed controller rejects further compaction attempts.</summary>
    [Fact]
    public async Task TryTriggerNowAsyncThrowsAfterDispose()
    {
        var opt = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 1000 };
        using var manifestStore = new Ledger(opt);
        await using var journal = JournalCoordinatorFactory.Create(opt, new State(), manifestStore, new AsyncManualResetEvent(true));
        var controller = new JournalCompactionController(opt, manifestStore, StoreFactory.CreateReader(opt), journal, NullLogger<JournalCompactionController>.Instance);
        controller.Dispose();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(controller.TryTriggerAsync(DefaultCancellationToken));
    }
}
