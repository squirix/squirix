using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Concurrency and lifecycle tests for <see cref="JournalCompactionController" />.
/// </summary>
public sealed class JournalCompactionControllerTests : UnitTestBase
{
    /// <summary>Double dispose does not throw.</summary>
    [Fact]
    [SuppressMessage("Major Code Smell", "S2699:Tests should include assertions", Justification = "This lifecycle test asserts that the second Dispose call does not throw.")]
    [SuppressMessage("ReSharper", "DisposeOnUsingVariable", Justification = "Dispose must be called two times")]
    public async Task DisposeIsIdempotent()
    {
        using var dir = new TempDirectory("squirix-journal-compact-ctrl-double");
        var opt = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 1000 };
        using var manifestStore = new ManifestStore(opt);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(opt, new Manifest(), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        using var controller = new JournalCompactionController(opt, manifestStore, journal, NullLogger<JournalCompactionController>.Instance);
        controller.Dispose();
    }

    /// <summary>
    /// When the controller compaction mutex is already held, <see cref="JournalCompactionController.TryTriggerNowAsync" /> returns false without waiting.
    /// </summary>
    [Fact]
    public async Task TryTriggerNowAsyncReturnsFalseWhenControllerMutexIsUnavailable()
    {
        using var dir = new TempDirectory("squirix-journal-compact-ctrl-mutex");
        var opt = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 16,
            FlushIntervalMs = 1000,
        };

        using var manifestStore = new ManifestStore(opt);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(opt, new Manifest(), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        await journal.AppendPutAsync(CacheKey.Default("gate"), [.. """{"v":{"$t":"s","v":"x"},"ver":1}"""u8], null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        using var controller = new JournalCompactionController(opt, manifestStore, journal, NullLogger<JournalCompactionController>.Instance);
        var mutexField = typeof(JournalCompactionController).GetField("_mutex", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(mutexField);
        var mutex = Assert.IsType<SemaphoreSlim>(mutexField.GetValue(controller));
        await mutex.WaitAsync(DefaultCancellationToken);
        try
        {
            Assert.False(await controller.TryTriggerNowAsync(DefaultCancellationToken));
        }
        finally
        {
            _ = mutex.Release();
        }

        Assert.True(await controller.TryTriggerNowAsync(DefaultCancellationToken));
    }

    /// <summary>Disposed controller rejects further compaction attempts.</summary>
    [Fact]
    public async Task TryTriggerNowAsyncThrowsAfterDispose()
    {
        using var dir = new TempDirectory("squirix-journal-compact-ctrl-dispose");
        var opt = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 1000 };
        using var manifestStore = new ManifestStore(opt);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(opt, new Manifest(), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var controller = new JournalCompactionController(opt, manifestStore, journal, NullLogger<JournalCompactionController>.Instance);
        controller.Dispose();

        _ = await Assert.ThrowsAsync<ObjectDisposedException>(async () => { _ = await controller.TryTriggerNowAsync(DefaultCancellationToken); });
    }
}
