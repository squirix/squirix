using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Concurrency and lifecycle tests for <see cref="JournalCompactionController" />.
/// </summary>
public sealed class JournalCompactionControllerTests : ServerUnitTestBase
{
    /// <summary>
    /// Double dispose does not throw.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    [SuppressMessage("ReSharper", "DisposeOnUsingVariable", Justification = "Dispose must be called two times")]
    public async Task DisposeIsIdempotent()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-compact-ctrl-double");
        try
        {
            var opt = new PersistenceOptions { DataDir = dir, StrictFsync = true, FlushIntervalMs = 1000 };
            var manifestStore = new ManifestStore(opt);
            await using var journal = new JournalWriter(opt, new Manifest(), manifestStore, new JournalStartupGate());
            using var controller = new JournalCompactionController(opt, manifestStore, journal, NullLogger<JournalCompactionController>.Instance);
            controller.Dispose();
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// When the controller compaction mutex is already held, <see cref="JournalCompactionController.TryTriggerNowAsync" /> returns false without waiting.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task TryTriggerNowAsyncReturnsFalseWhenControllerMutexIsUnavailable()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-compact-ctrl-mutex");
        try
        {
            var opt = new PersistenceOptions
            {
                DataDir = dir,
                StrictFsync = true,
                JournalMaxSegmentMb = 16,
                FlushIntervalMs = 1000,
            };

            var manifestStore = new ManifestStore(opt);
            await using var journal = new JournalWriter(opt, new Manifest(), manifestStore, new JournalStartupGate());
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
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Disposed controller rejects further compaction attempts.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task TryTriggerNowAsyncThrowsAfterDispose()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-compact-ctrl-dispose");
        try
        {
            var opt = new PersistenceOptions { DataDir = dir, StrictFsync = true, FlushIntervalMs = 1000 };
            var manifestStore = new ManifestStore(opt);
            await using var journal = new JournalWriter(opt, new Manifest(), manifestStore, new JournalStartupGate());
            var controller = new JournalCompactionController(opt, manifestStore, journal, NullLogger<JournalCompactionController>.Instance);
            controller.Dispose();

            _ = await Assert.ThrowsAsync<ObjectDisposedException>(async () => { _ = await controller.TryTriggerNowAsync(DefaultCancellationToken); });
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
