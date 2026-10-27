using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Sanity checks for FakeTimeProvider semantics used by expiration tests.</summary>
public sealed class FakeClockSanityTests
{
    /// <summary>Verifies Advance accumulates forward, rejects negative deltas, and SetUtcNow permits forward jumps only.</summary>
    [Test]
    public async Task AdvanceAndResetSemantics()
    {
        var f = new FakeTimeProvider();
        var start = f.GetUtcNow();

        f.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(f.GetUtcNow()).IsEqualTo(start.AddSeconds(1));

        // simulate accumulation across tests
        f.Advance(TimeSpan.FromSeconds(50));
        _ = await Assert.That(f.GetUtcNow()).IsEqualTo(start.AddSeconds(51));

        // time only moves forward through Advance
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(f, static clock => clock.Advance(TimeSpan.FromSeconds(-1)));

        // SetUtcNow jumps forward but also refuses to travel into the past
        f.SetUtcNow(start.AddMinutes(2));
        _ = await Assert.That(f.GetUtcNow()).IsEqualTo(start.AddMinutes(2));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(f, static clock => clock.SetUtcNow(clock.GetUtcNow().AddSeconds(-1)));
    }
}
