using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The test election timing refuses settings the elections cannot run with.</summary>
public sealed class TestElectionTimingTests
{
    /// <summary>A heartbeat that is not below the election timeout is refused.</summary>
    [Test]
    public async Task SlowHeartbeatIsRefused()
    {
        var timing = new TestElectionTiming { HeartbeatInterval = TimeSpan.FromSeconds(2) };

        var refused = NodeExceptionAssert.For<InvalidOperationException>().Throws(timing, static t => _ = t.ToOptions("node-a"));

        _ = await Assert.That(refused.Message).Contains("heartbeat below the election timeout", StringComparison.Ordinal);
    }

    /// <summary>A negative jitter is refused.</summary>
    [Test]
    public async Task NegativeJitterIsRefused()
    {
        var timing = new TestElectionTiming { MaxJitter = TimeSpan.FromMilliseconds(-1) };

        var refused = NodeExceptionAssert.For<InvalidOperationException>().Throws(timing, static t => _ = t.ToOptions("node-a"));

        _ = await Assert.That(refused.Message).Contains("non-negative jitter", StringComparison.Ordinal);
    }

    /// <summary>An unbounded wait for a leader is refused, as the product refuses it.</summary>
    [Test]
    public async Task UnboundedLeaderWaitIsRefused()
    {
        var timing = new TestElectionTiming { LeaderWaitTimeout = Timeout.InfiniteTimeSpan };

        var refused = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(timing, static t => _ = t.ToOptions("node-a"));

        _ = await Assert.That(refused.Message).Contains("bounded", StringComparison.Ordinal);
    }
}
