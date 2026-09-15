using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Deterministic election timeout driven by an injected time provider.</summary>
[Immutable]
public sealed class ElectionTimerTests : ServerUnitTestBase
{
    /// <summary>Starting a disposed timer throws instead of arming a new callback.</summary>
    [Test]
    public async Task StartAfterDisposeThrows()
    {
        var timer = ElectionTimer.Create(3, null, new FakeTimeProvider());
        _ = await Assert.That(timer).IsNotNull();
        timer.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(timer, static t => t.Start(static () => { }));
    }

    /// <summary>Advancing a fake time provider fires the election timeout without real waiting.</summary>
    [Test]
    public async Task UsesTimeProviderForDeterministicElection()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(150) };
        using var timer = ElectionTimer.Create(3, options, time);
        _ = await Assert.That(timer).IsNotNull();

        var firings = 0;
        timer.Start(() => { firings++; });

        time.Advance(TimeSpan.FromMilliseconds(149));
        _ = await Assert.That(firings).IsEqualTo(0);

        time.Advance(TimeSpan.FromMilliseconds(1));
        _ = await Assert.That(firings).IsEqualTo(1);

        timer.Reset();
        time.Advance(TimeSpan.FromMilliseconds(149));
        _ = await Assert.That(firings).IsEqualTo(1);

        time.Advance(TimeSpan.FromMilliseconds(1));
        _ = await Assert.That(firings).IsEqualTo(2);
    }
}
