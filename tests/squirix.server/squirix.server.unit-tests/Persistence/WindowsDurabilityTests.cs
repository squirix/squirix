using System;
using System.IO;
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
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

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
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

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
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 10 }, cancellationToken);
        _ = await Assert.That(await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, cancellationToken)).IsEqualTo(2);
    }

    /// <summary>Verifies that an empty current pointer, as left by a crash during the first publish, reads as never published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyPointerReadsAsDefault(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, "man-current"), ReadOnlyMemory<byte>.Empty, cancellationToken);

        var manifest = await store.ReadCurrentOrDefaultAsync(cancellationToken);

        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(1);
    }

    /// <summary>Verifies that a pointer of an earlier build is refused and left unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierPointerIsRefusedUnchanged(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        var path = NodePathKit.Combine(Dir, "man-current");
        byte[] earlier = [0x53, 0x51, 0x4D, 0x43, 0x2A, 0x00, 0x00, 0x00, 0x5D, 0xB7, 0x56, 0x95];
        await File.WriteAllBytesAsync(path, earlier, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadCurrentOrDefaultAsync(cancellationToken));

        await SequenceAssert.EqualMemoryAsync<byte>(earlier, await File.ReadAllBytesAsync(path, cancellationToken));
    }

    /// <summary>Verifies that a pointer without a valid slot fails loudly and is left unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PointerWithoutValidSlotFailsLoudly(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        var path = NodePathKit.Combine(Dir, "man-current");
        var image = GC.AllocateArray<byte>(PointerSlots.FileSize);
        PointerSlots.Encode(image, 1UL, 5);
        PointerSlots.Encode(image.AsSpan(PointerSlots.SlotSize), 2UL, 6);
        image[PointerSlots.RecordSize - 1] ^= 0xFF;
        image[PointerSlots.SlotSize + PointerSlots.RecordSize - 1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, image, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadCurrentOrDefaultAsync(cancellationToken));

        await SequenceAssert.EqualMemoryAsync<byte>(image, await File.ReadAllBytesAsync(path, cancellationToken));
    }

    /// <summary>Verifies that a missing current pointer target is treated as storage corruption.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowsWhenPointerTargetVanishes(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        await WriteCurrentPointerAsync(Dir, 123, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<FileNotFoundException>(store.ReadCurrentOrDefaultAsync(cancellationToken));
    }

    private static Task WriteCurrentPointerAsync(TempDirectory dir, int manifestIndex, CancellationToken cancellationToken)
    {
        var image = GC.AllocateArray<byte>(PointerSlots.FileSize);
        PointerSlots.Encode(image, 1UL, manifestIndex);
        return File.WriteAllBytesAsync(NodePathKit.Combine(dir, "man-current"), image, cancellationToken);
    }
}
