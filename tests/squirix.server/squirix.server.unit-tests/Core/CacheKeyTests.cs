using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Tests the value equality of <see cref="CacheKey" />, which the stores and indexes keyed by it rely on.</summary>
[Immutable]
public sealed class CacheKeyTests : ServerUnitTestBase
{
    private const string ItemKey = "item";
    private const string OrdersNamespace = "orders";

    /// <summary>The default key compares and hashes without throwing, and differs from a key of two empty strings.</summary>
    [Test]
    public async Task DefaultKeyComparesWithoutThrowing()
    {
        var empty = new CacheKey(string.Empty, string.Empty);

        _ = await Assert.That(default(CacheKey)).IsEqualTo(default);
        _ = await Assert.That(default(CacheKey).GetHashCode()).IsEqualTo(default(CacheKey).GetHashCode());
        _ = await Assert.That(default(CacheKey)).IsNotEqualTo(empty);
    }

    /// <summary>Equality is ordinal over both parts: case differs, and a swapped namespace and key differ.</summary>
    [Test]
    public async Task EqualityIsOrdinalOverBothParts()
    {
        var key = new CacheKey(OrdersNamespace, ItemKey);

        _ = await Assert.That(new CacheKey(OrdersNamespace, "ITEM")).IsNotEqualTo(key);
        _ = await Assert.That(new CacheKey(ItemKey, OrdersNamespace)).IsNotEqualTo(key);
        _ = await Assert.That(new CacheKey(OrdersNamespace, ItemKey + " ")).IsNotEqualTo(key);
    }

    /// <summary>Keys built from distinct string instances with the same characters are equal, hash alike and find the same dictionary slot.</summary>
    [Test]
    public async Task KeysWithEqualStringsAreEqual()
    {
        var first = new CacheKey(OrdersNamespace, ItemKey);
        var second = new CacheKey(new string(OrdersNamespace.ToCharArray()), new string(ItemKey.ToCharArray()));
        var map = new Dictionary<CacheKey, int> { [first] = 1 };

        _ = await Assert.That(second).IsEqualTo(first);
        _ = await Assert.That(second.GetHashCode()).IsEqualTo(first.GetHashCode());
        _ = await Assert.That(map.TryGetValue(second, out var found) && found == 1).IsTrue();
    }

    /// <summary>The text form joins the namespace and the key with a colon, and is the key alone in the empty namespace.</summary>
    [Test]
    public async Task ToStringJoinsNamespaceAndKey()
    {
        _ = await Assert.That(new CacheKey(OrdersNamespace, ItemKey).ToString()).IsEqualTo("orders:item");
        _ = await Assert.That(new CacheKey(string.Empty, ItemKey).ToString()).IsEqualTo(ItemKey);
    }
}
