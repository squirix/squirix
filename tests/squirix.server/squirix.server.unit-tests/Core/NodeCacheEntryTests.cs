using System.Text.Json;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Tests for value normalization on <see cref="NodeCacheEntry{T}" />.</summary>
public sealed class NodeCacheEntryTests : ServerUnitTestBase
{
    /// <summary>
    /// <see cref="NodeCacheEntry{T}.Normalize" /> keeps directly-encodable values unchanged and
    /// serializes arbitrary objects to a <see cref="JsonElement" />.
    /// </summary>
    [Fact]
    public void NormalizePreservesEncodables()
    {
        Assert.Null(new NodeCacheEntry<object?>(null).Normalize());
        Assert.True(Assert.IsType<bool>(new NodeCacheEntry<object?>(true).Normalize()));
        Assert.Equal("x", new NodeCacheEntry<object?>("x").Normalize());
        byte[] bytes = [1, 2];
        Assert.Same(bytes, new NodeCacheEntry<object?>(bytes).Normalize());
        const sbyte tiny = 3;
        Assert.Equal(tiny, new NodeCacheEntry<object?>(tiny).Normalize());
        Assert.Equal(4m, new NodeCacheEntry<object?>(4m).Normalize());

        var normalized = new NodeCacheEntry<object?>(new { Id = 1 }).Normalize();
        var element = Assert.IsType<JsonElement>(normalized);
        Assert.True(element.TryGetProperty("Id", out var id) || element.TryGetProperty("id", out id));
        Assert.Equal(1, id.GetInt32());
    }
}
