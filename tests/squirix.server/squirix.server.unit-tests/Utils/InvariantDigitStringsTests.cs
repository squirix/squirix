using System;
using System.Globalization;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers cached invariant digit formatting helpers.</summary>
public sealed class InvariantDigitStringsTests : ServerUnitTestBase
{
    /// <summary>D6 formatting uses the cache for indexes under 10000 and pads larger values.</summary>
    [Fact]
    public void FormatD6PadsSegmentIndexes()
    {
        Assert.Equal("000000", InvariantDigitStrings.FormatD6(0));
        Assert.Equal("000042", InvariantDigitStrings.FormatD6(42));
        Assert.Equal("010000", InvariantDigitStrings.FormatD6(10_000));
        Assert.Equal((-1).ToString("D6", CultureInfo.InvariantCulture), InvariantDigitStrings.FormatD6(-1));
    }

    /// <summary>Double formatting uses invariant G17.</summary>
    [Fact]
    public void FormatDoubleUsesInvariantG17()
    {
        const double value = 12.5d;
        Assert.Equal(value.ToString("G17", CultureInfo.InvariantCulture), InvariantDigitStrings.Format(value));
    }

    /// <summary>HTTPS origin formatting builds a single absolute URL string.</summary>
    [Fact]
    public void FormatHttpsOriginBuildsAbsoluteUrl()
    {
        Assert.Equal("https://localhost:5001", InvariantDigitStrings.FormatHttpsOrigin("localhost", 5001));
        Assert.Equal("https://127.0.0.1:0", InvariantDigitStrings.FormatHttpsOrigin("127.0.0.1", 0));
        Assert.Equal("https://host:-1", InvariantDigitStrings.FormatHttpsOrigin("host", -1));
        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(default(string?), static host => _ = InvariantDigitStrings.FormatHttpsOrigin(host!, 80));
    }

    /// <summary>Cached non-negative ints and longs reuse interned strings; out-of-range values format normally.</summary>
    [Fact]
    public void FormatUsesCacheForSmallNonNegativeValues()
    {
        Assert.Same(InvariantDigitStrings.Format(0), InvariantDigitStrings.Format(0L));
        Assert.Same(InvariantDigitStrings.Format(42), InvariantDigitStrings.Format(42UL));
        Assert.Equal("-3", InvariantDigitStrings.Format(-3));
        Assert.Equal("-9", InvariantDigitStrings.Format(-9L));
        Assert.Equal("2048", InvariantDigitStrings.Format(2048));
        Assert.Equal("5000", InvariantDigitStrings.Format(5000L));
        Assert.Equal("5000", InvariantDigitStrings.Format(5000UL));
    }
}
