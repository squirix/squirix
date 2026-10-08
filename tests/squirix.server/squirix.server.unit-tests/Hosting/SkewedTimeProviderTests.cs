using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The skewed node clock moves only its wall time: timestamps and timers stay the system ones.</summary>
public sealed class SkewedTimeProviderTests
{
    private static readonly TimeSpan Offset = TimeSpan.FromSeconds(-30);

    /// <summary>The wall time runs the offset away from the system wall time.</summary>
    [Test]
    public async Task WallTimeRunsAtTheOffset()
    {
        var clock = new SkewedTimeProvider(Offset);

        var before = TimeProvider.System.GetUtcNow();
        var skewed = clock.GetUtcNow();
        var after = TimeProvider.System.GetUtcNow();

        _ = await Assert.That(skewed).IsGreaterThanOrEqualTo(before + Offset);
        _ = await Assert.That(skewed).IsLessThanOrEqualTo(after + Offset);
    }

    /// <summary>Timestamps keep the system frequency and source, so elapsed time is unaffected by the offset.</summary>
    [Test]
    public async Task TimestampsStayTheSystemOnes()
    {
        var clock = new SkewedTimeProvider(Offset);

        var before = TimeProvider.System.GetTimestamp();
        var skewed = clock.GetTimestamp();
        var after = TimeProvider.System.GetTimestamp();

        _ = await Assert.That(clock.TimestampFrequency).IsEqualTo(TimeProvider.System.TimestampFrequency);
        _ = await Assert.That(skewed).IsBetween(before, after);
    }

    /// <summary>A timer of the skewed clock fires in real time, ignoring the offset of the wall time.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimerFiresInRealTime(CancellationToken cancellationToken)
    {
        var clock = new SkewedTimeProvider(TimeSpan.FromDays(1));
        var fired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using (clock.CreateTimer(_ => fired.TrySetResult(), null, TimeSpan.FromMilliseconds(10), Timeout.InfiniteTimeSpan))
            await fired.Task.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(fired.Task.IsCompletedSuccessfully).IsTrue();
    }
}
