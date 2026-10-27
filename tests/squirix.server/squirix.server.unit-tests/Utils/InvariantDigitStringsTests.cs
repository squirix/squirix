using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Covers cached invariant digit formatting helpers.</summary>
[Immutable]
public sealed class InvariantDigitStringsTests : ServerUnitTestBase
{
    /// <summary>D6 formatting uses the cache for indexes under 10000 and pads larger values.</summary>
    [Test]
    public async Task FormatD6PadsSegmentIndexes()
    {
        _ = await Assert.That(InvariantDigitStrings.FormatD6(0)).IsEqualTo("000000");
        _ = await Assert.That(InvariantDigitStrings.FormatD6(42)).IsEqualTo("000042");
        _ = await Assert.That(InvariantDigitStrings.FormatD6(10_000)).IsEqualTo("010000");
        _ = await Assert.That(InvariantDigitStrings.FormatD6(-1)).IsEqualTo((-1).ToString("D6", CultureInfo.InvariantCulture));
    }

    /// <summary>Double formatting uses invariant G17.</summary>
    [Test]
    public async Task FormatDoubleUsesInvariantG17()
    {
        const double value = 12.5d;
        _ = await Assert.That(InvariantDigitStrings.Format(value)).IsEqualTo(value.ToString("G17", CultureInfo.InvariantCulture));
    }

    /// <summary>HTTPS origin formatting builds a single absolute URL string.</summary>
    [Test]
    public async Task FormatHttpsOriginBuildsAbsoluteUrl()
    {
        _ = await Assert.That(InvariantDigitStrings.FormatHttpsOrigin("localhost", 5001)).IsEqualTo("https://localhost:5001");
        _ = await Assert.That(InvariantDigitStrings.FormatHttpsOrigin("127.0.0.1", 0)).IsEqualTo("https://127.0.0.1:0");
        _ = await Assert.That(InvariantDigitStrings.FormatHttpsOrigin("host", -1)).IsEqualTo("https://host:-1");
        _ = await Assert.That(InvariantDigitStrings.FormatHttpsOrigin("::1", 443)).IsEqualTo("https://[::1]:443");
        _ = await Assert.That(InvariantDigitStrings.FormatHttpsOrigin("2001:db8::1", 8443)).IsEqualTo("https://[2001:db8::1]:8443");
        _ = await Assert.That(new Uri(InvariantDigitStrings.FormatHttpsOrigin("::1", 443)).AbsoluteUri).IsEqualTo(new Uri("https://[::1]:443").AbsoluteUri);
        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(default(string?), static host => _ = InvariantDigitStrings.FormatHttpsOrigin(host!, 80));
    }

    /// <summary>Cached non-negative ints and longs reuse interned strings; out-of-range values format normally.</summary>
    [Test]
    public async Task FormatUsesCacheForSmallNonNegativeValues()
    {
        _ = await Assert.That(InvariantDigitStrings.Format(0L)).IsSameReferenceAs(InvariantDigitStrings.Format(0));
        _ = await Assert.That(InvariantDigitStrings.Format(42UL)).IsSameReferenceAs(InvariantDigitStrings.Format(42));
        _ = await Assert.That(InvariantDigitStrings.Format(-3)).IsEqualTo("-3");
        _ = await Assert.That(InvariantDigitStrings.Format(-9L)).IsEqualTo("-9");
        _ = await Assert.That(InvariantDigitStrings.Format(2048)).IsEqualTo("2048");
        _ = await Assert.That(InvariantDigitStrings.Format(5000L)).IsEqualTo("5000");
        _ = await Assert.That(InvariantDigitStrings.Format(5000UL)).IsEqualTo("5000");
    }
}
