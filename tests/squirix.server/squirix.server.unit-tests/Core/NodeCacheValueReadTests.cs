using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Tests that <see cref="NodeCacheValueRead{T}" /> keeps the hit flag and the deadline apart in its one state field.</summary>
[Immutable]
public sealed class NodeCacheValueReadTests : ServerUnitTestBase
{
    /// <summary>A miss, the default value included, reads as absent with no value and no deadline, whatever was passed for them.</summary>
    [Test]
    public async Task MissHasNoValueOrDeadline()
    {
        var miss = new NodeCacheValueRead<string>(false, "ignored", new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        _ = await Assert.That(miss.Found).IsFalse();
        _ = await Assert.That(miss.Value).IsNull();
        _ = await Assert.That(miss.ExpiresUtc).IsNull();
        _ = await Assert.That(miss).IsEqualTo(default(NodeCacheValueRead<string>));
    }

    /// <summary>A hit keeps its value, a null value too, and its deadline, from the first tick to the last one a deadline can have.</summary>
    [Test]
    public async Task HitKeepsValueAndDeadline()
    {
        var noDeadline = new NodeCacheValueRead<string>(true, null, null);
        var first = new NodeCacheValueRead<string>(true, "v", DateTime.MinValue);
        var last = new NodeCacheValueRead<string>(true, "v", DateTime.MaxValue);

        _ = await Assert.That(noDeadline.Found).IsTrue();
        _ = await Assert.That(noDeadline.Value).IsNull();
        _ = await Assert.That(noDeadline.ExpiresUtc).IsNull();
        _ = await Assert.That((first.Found, first.Value, first.ExpiresUtc)).IsEqualTo((true, "v", DateTime.MinValue));
        _ = await Assert.That(last.ExpiresUtc).IsEqualTo(DateTime.MaxValue);
        _ = await Assert.That(last.ExpiresUtc?.Kind).IsEqualTo(DateTimeKind.Utc);
    }
}
