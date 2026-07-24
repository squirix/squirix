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

    /// <summary>Numeric formatters use invariant culture.</summary>
    [Fact]
    public void NumericFormattersUseInvariantCulture()
    {
        Assert.Equal("42", ActivityTagValues.Int32(42));
        Assert.Equal("-7", ActivityTagValues.Int64(-7));
        Assert.Equal("1.5", ActivityTagValues.Double(1.5));
    }
}
