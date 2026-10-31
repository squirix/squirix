using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Extensions see structured values as <see cref="JsonElement" />, whatever form the node stores them in.</summary>
[Immutable]
public sealed class BasicExtensionCachePipelineAdapterTests : ServerUnitTestBase
{
    private const string ObjectText = """{"id":7,"tags":["a"]}""";
    private const string CacheName = "cache";
    private const string Key = "key";

    /// <summary>An entry read through the adapter carries the element and keeps its metadata.</summary>
    [Test]
    public async Task EntryValueIsShownAsElement()
    {
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var stored = new NodeCacheEntry<object?>(JsonValueKit.Create(ObjectText), 5, deadline, TimeSpan.FromMinutes(1));
        var inner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<object?>?>(stored));
        var adapter = new BasicExtensionCachePipelineAdapter<object?>(inner.Instance());

        var entry = await adapter.GetEntryAsync(CacheName, Key, CancellationToken.None);

        var element = await Assert.That(entry!.Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.GetProperty("id").GetInt32()).IsEqualTo(7);
        _ = await Assert.That(entry.Version).IsEqualTo(5);
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(deadline);
        _ = await Assert.That(entry.Expiration).IsEqualTo(TimeSpan.FromMinutes(1));
    }

    /// <summary>A value read through the adapter is the element.</summary>
    [Test]
    public async Task ValueIsShownAsElement()
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = inner.Setups.GetValueAsync(CacheName, Key, Arg.Any<CancellationToken>())
                 .ReturnValue(ValueTask.FromResult(new NodeCacheValueResult<object?>(true, JsonValueKit.Create(ObjectText))));
        var adapter = new BasicExtensionCachePipelineAdapter<object?>(inner.Instance());

        var result = await adapter.GetValueAsync(CacheName, Key, CancellationToken.None);

        var element = await Assert.That(result.Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(element.GetRawText()).IsEqualTo(ObjectText);
    }

    /// <summary>The value of a removed entry is the element.</summary>
    [Test]
    public async Task RemovedValueIsShownAsElement()
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = inner.Setups.RemoveAsync("op", CacheName, Key, Arg.Any<CancellationToken>())
                 .ReturnValue(ValueTask.FromResult(new CacheRemoveResult<object?>(true, JsonValueKit.Create(ObjectText))));
        var adapter = new BasicExtensionCachePipelineAdapter<object?>(inner.Instance());

        var result = await adapter.RemoveAsync("op", CacheName, Key, CancellationToken.None);

        var element = await Assert.That(result.Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(result.Removed).IsTrue();
        _ = await Assert.That(element.GetProperty("tags").GetArrayLength()).IsEqualTo(1);
    }

    /// <summary>Primitive values and absent entries pass through the adapter untouched, without a copy of the entry.</summary>
    [Test]
    public async Task PrimitivesPassThroughUnchanged()
    {
        var stored = new NodeCacheEntry<object?>("text", 2);
        var inner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = inner.Setups.GetEntryAsync(CacheName, Key, Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<object?>?>(stored));
        _ = inner.Setups.GetEntryAsync(CacheName, "missing", Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<object?>?>(null));
        var adapter = new BasicExtensionCachePipelineAdapter<object?>(inner.Instance());

        _ = await Assert.That(await adapter.GetEntryAsync(CacheName, Key, CancellationToken.None)).IsSameReferenceAs(stored);
        _ = await Assert.That(await adapter.GetEntryAsync(CacheName, "missing", CancellationToken.None)).IsNull();
    }
}
