using System;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Deterministic election timeout driven by an injected time provider.</summary>
[Immutable]
public sealed class ElectionTimerTests : ServerUnitTestBase
{
    /// <summary>Advancing a fake time provider fires the election timeout without real waiting.</summary>
    [Fact]
    public void UsesTimeProviderForDeterministicElection()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(150) };
        using var timer = ElectionTimer.Create(3, options, time);
        Assert.NotNull(timer);

        var firings = 0;
        timer.Start(() => { firings++; });

        time.Advance(TimeSpan.FromMilliseconds(149));
        Assert.Equal(0, firings);

        time.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Equal(1, firings);

        timer.Reset();
        time.Advance(TimeSpan.FromMilliseconds(149));
        Assert.Equal(1, firings);

        time.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Equal(2, firings);
    }

    /// <summary>Starting a disposed timer throws instead of arming a new callback.</summary>
    [Fact]
    public void StartAfterDisposeThrows()
    {
        var timer = ElectionTimer.Create(3, null, new FakeTimeProvider());
        Assert.NotNull(timer);
        timer.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(
            timer,
            static t => t.Start(static () => { }));
    }
}
