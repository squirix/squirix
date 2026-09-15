using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Covers Activity tag string formatting helpers.</summary>
[Immutable]
public sealed class ActivityTagValuesTests : ServerUnitTestBase
{
    /// <summary>Bool formatting uses stable literals.</summary>
    [Test]
    public async Task BoolFormatsStableLiterals()
    {
        _ = await Assert.That(ActivityTagValues.Bool(true)).IsEqualTo(ActivityTagValues.True);
        _ = await Assert.That(ActivityTagValues.Bool(false)).IsEqualTo(ActivityTagValues.False);
    }

    /// <summary>Double formatting delegates to invariant digit helpers.</summary>
    [Test]
    public async Task DoubleFormatsInvariantValue() => _ = await Assert.That(ActivityTagValues.Double(1.5d)).IsEqualTo("1.5");

    /// <summary>Cached non-negative integers reuse interned digit strings.</summary>
    [Test]
    public async Task NonNegativeIntegersReuseCachedStrings()
    {
        _ = await Assert.That(ActivityTagValues.Int32(0)).IsSameReferenceAs(ActivityTagValues.Int32(0));
        _ = await Assert.That(ActivityTagValues.Int64(42)).IsSameReferenceAs(ActivityTagValues.Int32(42));
        _ = await Assert.That(ActivityTagValues.UInt64(42)).IsSameReferenceAs(ActivityTagValues.Int32(42));
        _ = await Assert.That(ActivityTagValues.Int64(-7)).IsEqualTo("-7");
        _ = await Assert.That(ActivityTagValues.Int32(2048)).IsEqualTo("2048");
    }
}
