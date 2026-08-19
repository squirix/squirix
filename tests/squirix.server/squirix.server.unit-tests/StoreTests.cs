using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Integration tests for the manifest store.</summary>
public sealed class StoreTests : ServerUnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    private TempDirectory Dir => _dir ?? throw new InvalidOperationException("Test directory is not initialized.");

    /// <summary>Verifies sequential roll publishes advance the current pointer while a persistent handle stays open.</summary>
    [Fact]
    public async Task EnqueueRollAdvancesCurrentPointerSequentially()
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        await RollAsync();
        using var reloaded = new Ledger(options);
        Assert.Equal(2, (await reloaded.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }

        async Task RollAsync()
        {
            using var store = new Ledger(options);
            Exception? rollError = null;
            store.EnqueueRoll(1, 1, static () => { }, ex => rollError = ex);
            store.EnqueueRoll(2, 2, static () => { }, ex => rollError = ex);

            await StoreTestSupport.WaitUntilAsync(store, (Func<Ledger, CancellationToken, ValueTask<bool>>)ConditionAsync, DefaultCancellationToken);
            StoreTestSupport.ThrowIfFaulted(rollError);
            Assert.Equal(2, (await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        }
    }

    /// <summary>Verifies the first write creates a current pointer and numbered manifest file.</summary>
    [Fact]
    public async Task WriteAsyncCreatesCurrentPointerAndManifestFile()
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);

        var currentPath = NodePathKit.Combine(Dir.Path, "man-current");
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, DefaultCancellationToken);
        Assert.Equal(12, pointerBytes.Length);
        Assert.Equal(1, Pointer.Read(pointerBytes));

        var manifestPath = NodePathKit.Combine(Dir.Path, "man-000001.bmqx");
        Assert.True(File.Exists(manifestPath));
        var manifest = FileCodec.Decode(await File.ReadAllBytesAsync(manifestPath, DefaultCancellationToken));
        Assert.Equal(1, manifest.CurrentJournal);
        Assert.Equal(1UL, manifest.NextSequence);
    }

    /// <summary>Verifies CURRENT is updated in place without leaving a temp pointer file.</summary>
    [Fact]
    public async Task WriteAsyncUpdatesCurrentPointerInPlaceTmpFile()
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);

        Assert.False(File.Exists(NodePathKit.Combine(Dir.Path, "man-current.tmp")));
        Assert.Equal(12, (await File.ReadAllBytesAsync(NodePathKit.Combine(Dir.Path, "man-current"), DefaultCancellationToken)).Length);
    }

    /// <summary>Disposes the temporary directory after the test class finishes.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates a temporary directory for test storage.</summary>
    public ValueTask InitializeAsync()
    {
        _dir = new TempDirectory("manifest");
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _dir?.Dispose();

        base.Dispose(disposing);
    }
}
