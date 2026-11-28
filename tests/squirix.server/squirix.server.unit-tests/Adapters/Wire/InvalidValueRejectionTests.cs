using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>A value that is not valid JSON is refused before any durable effect, and a retry is refused the same way.</summary>
[Immutable]
public sealed class InvalidValueRejectionTests : IsolatedStorageTestBase
{
    private const string BrokenJson = "{\"a\":1,}";

    /// <summary>A refused set leaves no journal frame, no stored value and no flush, and its retry fails identically.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedSetLeavesNothingBehind(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var request = FusedWriteHarness.Set(FusedWriteHarness.OpId(1));
        request.Entry = request.Entry!.Value with { Value = JsonValueKit.Wire(BrokenJson) };
        var flushesBefore = harness.FlushCount;

        var first = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(harness.Adapter.Mutations.SetEntryAsync(request, CancellationToken.None).AsTask(), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var retry = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(harness.Adapter.Mutations.SetEntryAsync(request, CancellationToken.None).AsTask(), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(first.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(retry.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(retry.Message).IsEqualTo(first.Message);
        _ = await Assert.That(harness.FlushCount).IsEqualTo(flushesBefore);
        _ = await Assert.That(harness.ReadFrames(cancellationToken)).IsEmpty();
        _ = await Assert.That(harness.Physical.GetValue(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken).Found).IsFalse();
    }

    /// <summary>The other value-carrying writes refuse the value the same way and leave no frame.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedValueOnEveryWriteLeavesNoFrame(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        _ = await harness.Adapter.Mutations.SetEntryAsync(FusedWriteHarness.Set(FusedWriteHarness.OpId(1)), CancellationToken.None);
        var framesBefore = harness.ReadFrames(cancellationToken).Count;
        var update = FusedWriteHarness.Update(FusedWriteHarness.OpId(2), "x");
        update.Entry = update.Entry!.Value with { Value = JsonValueKit.Wire(BrokenJson) };
        var add = FusedWriteHarness.AddIfAbsent(FusedWriteHarness.OpId(3), "x", "other");
        add.Entry = add.Entry!.Value with { Value = JsonValueKit.Wire(BrokenJson) };
        var getOrAdd = FusedWriteHarness.GetOrAdd(FusedWriteHarness.OpId(4), "x", "another");
        getOrAdd.Entry = getOrAdd.Entry!.Value with { Value = JsonValueKit.Wire(BrokenJson) };

        var updateFailure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(harness.Adapter.Mutations.UpdateAsync(update, CancellationToken.None).AsTask(), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var addFailure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(harness.Adapter.Mutations.AddEntryIfAbsentAsync(add, CancellationToken.None).AsTask(), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var getOrAddFailure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(harness.Adapter.Mutations.GetOrAddAsync(getOrAdd, CancellationToken.None).AsTask(), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(updateFailure.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(addFailure.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(getOrAddFailure.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(harness.ReadFrames(cancellationToken).Count).IsEqualTo(framesBefore);
    }
}
