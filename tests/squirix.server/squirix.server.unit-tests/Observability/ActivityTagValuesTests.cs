using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Covers Activity tag string formatting helpers.</summary>
public sealed class ActivityTagValuesTests : ServerUnitTestBase
{
    /// <summary>Bool formatting uses stable literals.</summary>
    [Fact]
    public void BoolFormatsStableLiterals()
    {
        Assert.Equal(ActivityTagValues.True, ActivityTagValues.Bool(true));
        Assert.Equal(ActivityTagValues.False, ActivityTagValues.Bool(false));
    }

    /// <summary>Cached non-negative integers reuse interned digit strings.</summary>
    [Fact]
    public void NonNegativeIntegersReuseCachedStrings()
    {
        Assert.Same(ActivityTagValues.Int32(0), ActivityTagValues.Int32(0));
        Assert.Same(ActivityTagValues.Int32(42), ActivityTagValues.Int64(42));
        Assert.Same(ActivityTagValues.Int32(42), ActivityTagValues.UInt64(42));
        Assert.Equal("-7", ActivityTagValues.Int64(-7));
        Assert.Equal("2048", ActivityTagValues.Int32(2048));
    }

    /// <summary>Double formatting delegates to invariant digit helpers.</summary>
    [Fact]
    public void DoubleFormatsInvariantValue() => Assert.Equal("1.5", ActivityTagValues.Double(1.5d));
}
