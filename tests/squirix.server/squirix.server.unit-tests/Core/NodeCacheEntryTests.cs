using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Tests for value normalization on <see cref="NodeCacheEntry{T}" />.</summary>
[Immutable]
public sealed class NodeCacheEntryTests : ServerUnitTestBase
{
    /// <summary>
    /// <see cref="NodeCacheEntry{T}.Normalize" /> keeps directly encodable values unchanged and
    /// serializes arbitrary objects to a <see cref="JsonElement" />.
    /// </summary>
    [Test]
    public async Task NormalizePreservesData()
    {
        _ = await Assert.That(new NodeCacheEntry<object?>(null).Normalize()).IsNull();
        var normalizedBool = new NodeCacheEntry<object?>(true).Normalize();
        _ = await Assert.That(normalizedBool).IsTypeOf<bool>();
        _ = await Assert.That(normalizedBool is true).IsTrue();
        _ = await Assert.That(new NodeCacheEntry<object?>("x").Normalize()).IsEqualTo("x");
        byte[] bytes = [1, 2];
        _ = await Assert.That(new NodeCacheEntry<object?>(bytes).Normalize()).IsSameReferenceAs(bytes);
        const sbyte tiny = 3;
        _ = await Assert.That(new NodeCacheEntry<object?>(tiny).Normalize()).IsEqualTo(tiny);
        _ = await Assert.That(new NodeCacheEntry<object?>(4m).Normalize()).IsEqualTo(4m);

        var normalized = new NodeCacheEntry<object?>(new IdPayload { Id = 1 }).Normalize();
        var element = await Assert.That(normalized).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.TryGetProperty("Id", out var id) || element.TryGetProperty("id", out id)).IsTrue();
        _ = await Assert.That(id.GetInt32()).IsEqualTo(1);
    }

    /// <summary>
    /// <see cref="NodeCacheEntry{T}.Normalize" /> serializes the runtime type, not the declared entry type,
    /// so derived properties on a base/interface-declared entry survive normalization.
    /// </summary>
    [Test]
    public async Task NormalizeSerializesDerivedValueType()
    {
        var entry = new NodeCacheEntry<IValueContract>(new DerivedValue { DerivedField = "survives" });
        var normalized = entry.Normalize();
        var element = await Assert.That(normalized).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.TryGetProperty("DerivedField", out var field) || element.TryGetProperty("derivedField", out field)).IsTrue();
        _ = await Assert.That(field.GetString()).IsEqualTo("survives");
    }
}
