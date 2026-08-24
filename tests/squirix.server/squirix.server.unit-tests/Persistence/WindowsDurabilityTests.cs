using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Durability behavior tests for manifest persistence and CURRENT pointer updates.</summary>
public sealed class WindowsDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>
    /// Verifies that <see cref="Ledger" /> creates an initial manifest and updates the CURRENT pointer.
    /// </summary>
    [Fact]
    public async Task FirstWriteCreatesCurrentPointer()
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);
        var currentPath = NodePathKit.Combine(Dir, "man-current");
        Assert.True(File.Exists(currentPath));
        Assert.Equal(1, await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, DefaultCancellationToken));
    }

    /// <summary>Verifies that first boot without a current pointer returns a default manifest.</summary>
    [Fact]
    public async Task MissingPointerReadsAsDefault()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options);

        var manifest = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);

        Assert.Equal(1, manifest.CurrentJournal);
        Assert.Equal(1UL, manifest.NextSequence);
    }

    /// <summary>Verifies that a missing current pointer target is treated as storage corruption.</summary>
    [Fact]
    public async Task ThrowsWhenPointerTargetVanishes()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options);
        WriteCurrentPointer(Dir, 123);

        _ = await NodeAsyncAssert.ThrowsAsync<FileNotFoundException>(store.ReadCurrentOrDefaultAsync(DefaultCancellationToken));
    }

    /// <summary>Verifies that an empty current pointer is treated as storage corruption.</summary>
    [Fact]
    public async Task ThrowsWhenCurrentPointerIsEmpty()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options);
        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, "man-current"), ReadOnlyMemory<byte>.Empty, DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadCurrentOrDefaultAsync(DefaultCancellationToken));
    }

    /// <summary>Verifies that subsequent manifest writes update the CURRENT pointer to the new manifest file.</summary>
    [Fact]
    public async Task RewriteUpdatesCurrentPointer()
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 10 }, DefaultCancellationToken);
        Assert.Equal(2, await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, DefaultCancellationToken));
    }

    private static void WriteCurrentPointer(TempDirectory dir, int manifestIndex)
    {
        Span<byte> pointerBuffer = stackalloc byte[Pointer.Size];
        Pointer.Write(pointerBuffer, manifestIndex);
        File.WriteAllBytes(NodePathKit.Combine(dir, "man-current"), pointerBuffer);
    }
}
