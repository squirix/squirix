using System;
using System.IO;
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

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Durability behavior tests for manifest persistence and CURRENT pointer updates.</summary>
public sealed class WindowsDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>Verifies that <see cref="Ledger" /> creates an initial manifest and updates the CURRENT pointer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstWriteCreatesCurrentPointer(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        var currentPath = NodePathKit.Combine(Dir, "man-current");
        _ = await Assert.That(File.Exists(currentPath)).IsTrue();
        _ = await Assert.That(await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, cancellationToken)).IsEqualTo(1);
    }

    /// <summary>Verifies that first boot without a current pointer returns a default manifest.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingPointerReadsAsDefault(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options);

        var manifest = await store.ReadCurrentOrDefaultAsync(cancellationToken);

        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(1);
        _ = await Assert.That(manifest.NextSequence).IsEqualTo(1UL);
    }

    /// <summary>Verifies that subsequent manifest writes update the CURRENT pointer to the new manifest file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RewriteUpdatesCurrentPointer(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 10 }, cancellationToken);
        _ = await Assert.That(await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, cancellationToken)).IsEqualTo(2);
    }

    /// <summary>Verifies that an empty current pointer is treated as storage corruption.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowsWhenCurrentPointerIsEmpty(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options);
        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, "man-current"), ReadOnlyMemory<byte>.Empty, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadCurrentOrDefaultAsync(cancellationToken));
    }

    /// <summary>Verifies that a missing current pointer target is treated as storage corruption.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowsWhenPointerTargetVanishes(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options);
        WriteCurrentPointer(Dir, 123);

        _ = await NodeAsyncAssert.ThrowsAsync<FileNotFoundException>(store.ReadCurrentOrDefaultAsync(cancellationToken));
    }

    private static void WriteCurrentPointer(TempDirectory dir, int manifestIndex)
    {
        Span<byte> pointerBuffer = stackalloc byte[Pointer.Size];
        Pointer.Write(pointerBuffer, manifestIndex);
        File.WriteAllBytes(NodePathKit.Combine(dir, "man-current"), pointerBuffer);
    }
}
