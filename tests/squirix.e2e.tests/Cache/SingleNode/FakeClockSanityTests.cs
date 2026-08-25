using System;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Sanity checks for FakeTimeProvider semantics used by expiration tests.</summary>
public sealed class FakeClockSanityTests
{
    /// <summary>Verifies Advance accumulates forward, rejects negative deltas, and SetUtcNow permits forward jumps only.</summary>
    [Fact]
    public void AdvanceAndResetSemantics()
    {
        var f = new FakeTimeProvider();
        var start = f.GetUtcNow();

        f.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(start.AddSeconds(1), f.GetUtcNow());

        // simulate accumulation across tests
        f.Advance(TimeSpan.FromSeconds(50));
        Assert.Equal(start.AddSeconds(51), f.GetUtcNow());

        // time only moves forward through Advance
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(f, static clock => clock.Advance(TimeSpan.FromSeconds(-1)));

        // SetUtcNow jumps forward but also refuses to travel into the past
        f.SetUtcNow(start.AddMinutes(2));
        Assert.Equal(start.AddMinutes(2), f.GetUtcNow());
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(f, static clock => clock.SetUtcNow(clock.GetUtcNow().AddSeconds(-1)));
    }
}
