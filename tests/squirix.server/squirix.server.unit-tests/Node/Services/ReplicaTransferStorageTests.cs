using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaStateHarness;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Borrowed transfer bytes remain valid through writes that are already in flight when cancellation arrives.</summary>
public sealed class ReplicaTransferStorageTests : ServerUnitTestBase
{
    /// <summary>Cancellation waits for an accepted write; clearing its transport buffer afterward cannot corrupt the stored value or checksum.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationFinishesBorrowedWrite(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-borrowed-write");
        var hold = new HeldWrite();
        await using var harness = await OpenAsync(dir, cancellationToken, decorateSinkCache: hold.Wrap);
        _ = await harness.Sink.EnterAsync(GroupId, cancellationToken);
        var original = Entry("k-1", "stored-value", 0L);
        var transport = new byte[original.Key.Length + original.Payload.Length];
        original.Key.Span.CopyTo(transport);
        original.Payload.Span.CopyTo(transport.AsSpan(original.Key.Length));
        var borrowed = new ReplicaStateEntry(original.CacheName, transport.AsMemory(0, original.Key.Length), transport.AsMemory(original.Key.Length, original.Payload.Length), 0);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var writing = harness.Sink.WriteAsync(GroupId, new[] { borrowed, borrowed }, cancellation.Token).AsTask();
        try
        {
            await hold.Reached.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            await cancellation.CancelAsync();
            _ = await Assert.That(writing.IsCompleted).IsFalse();
        }
        finally
        {
            hold.Release();
        }

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(writing);
        transport.AsSpan().Fill(0xCC);
        var stored = await harness.Cache.GetEntryAsync(new CacheKey(CacheName, "k-1"), cancellationToken);
        var totals = await harness.Sink.SealAsync(GroupId, cancellationToken);

        _ = await Assert.That(stored?.Value).IsEqualTo("stored-value");
        _ = await Assert.That(totals).IsEqualTo(TotalsOf(original));
        harness.Sink.Leave(GroupId);
    }

    private sealed class HeldWrite
    {
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Reached => _reached.Task;

        internal void Release() => _ = _release.TrySetResult();

        internal ILogicalNamespacedCache<object?> Wrap(ILogicalNamespacedCache<object?> inner)
        {
            var rock = new ILogicalNamespacedCacheCreateExpectations<object?>();
            _ = rock.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
                .Callback(async (operation, cache, key, entry, length, token) =>
                {
                    _ = _reached.TrySetResult();
                    await _release.Task.ConfigureAwait(false);
                    await inner.SetEntryAsync(operation, cache, key, entry, length, token).ConfigureAwait(false);
                });
            return rock.Instance();
        }
    }
}
