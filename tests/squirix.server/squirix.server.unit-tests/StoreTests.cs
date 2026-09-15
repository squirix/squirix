using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Integration tests for the manifest store.</summary>
public sealed class StoreTests : IsolatedStorageTestBase
{
    /// <inheritdoc />
    protected override string TempDirectoryName => "manifest";

    /// <summary>Verifies sequential roll publishes advance the current pointer while a persistent handle stays open.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnqueueRollAdvancesPointerSequentially(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        await RollAsync();
        using var reloaded = new Ledger(options);
        _ = await Assert.That((await reloaded.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
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

            await store.WaitUntilValueAsync(ConditionAsync, cancellationToken);
            rollError.ThrowIfFaulted();
            _ = await Assert.That((await store.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        }
    }

    /// <summary>Verifies the first write creates a current pointer and numbered manifest file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteCreatesPointerAndManifestFile(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);

        var currentPath = NodePathKit.Combine(Dir.Path, "man-current");
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken);
        _ = await Assert.That(pointerBytes.Length).IsEqualTo(12);
        _ = await Assert.That(Pointer.Read(pointerBytes)).IsEqualTo(1);

        var manifestPath = NodePathKit.Combine(Dir.Path, "man-000001.bmqx");
        _ = await Assert.That(File.Exists(manifestPath)).IsTrue();
        var manifest = FileCodec.Decode(await File.ReadAllBytesAsync(manifestPath, cancellationToken));
        _ = await Assert.That(manifest.CurrentJournal).IsEqualTo(1);
        _ = await Assert.That(manifest.NextSequence).IsEqualTo(1UL);
    }

    /// <summary>Verifies CURRENT is updated in place without leaving a temp pointer file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteUpdatesPointerViaTempFile(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new Ledger(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);

        _ = await Assert.That(File.Exists(NodePathKit.Combine(Dir.Path, "man-current.tmp"))).IsFalse();
        _ = await Assert.That((await File.ReadAllBytesAsync(NodePathKit.Combine(Dir.Path, "man-current"), cancellationToken)).Length).IsEqualTo(12);
    }
}
