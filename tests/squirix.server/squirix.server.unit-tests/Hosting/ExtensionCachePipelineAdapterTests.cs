using System.Collections.Generic;
using System.Text;
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

/// <summary>Verifies extension cache pipeline adapter behavior.</summary>
/// <remarks>The core cache mocks have no setups: an entry-aware pipeline must take every operation, so any call to the core fails the test.</remarks>
[Immutable]
public sealed class ExtensionCachePipelineAdapterTests
{
    /// <summary>Ensures entry-aware extension pipelines receive entry operations.</summary>
    [Test]
    public async Task EntryOpsUseEntryAwarePipelineAsync()
    {
        NodeCacheEntry<object?>? stored = null;
        var insertEntryCalls = 0;
        var getEntryCalls = 0;
        var decorated = new ISquirixServerEntryCachePipelineCreateExpectations<object?>();
        _ = decorated.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                     .Callback((_, _, _, written, _) =>
                      {
                          insertEntryCalls++;
                          stored = written;
                          return ValueTask.CompletedTask;
                      });
        _ = decorated.Setups.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                     .Callback((_, _, _) =>
                      {
                          getEntryCalls++;
                          return ValueTask.FromResult(stored);
                      });
        var adapter = new ExtensionCachePipelineAdapter<object?>(new ILogicalNamespacedCacheCreateExpectations<object?>().Instance(), decorated.Instance());
        var entry = new NodeCacheEntry<object?> { Value = "value", Version = 7 };

        await adapter.SetEntryAsync(UnitMutationOpIds.Default, "cache", "key", entry, CancellationToken.None);
        var result = await adapter.GetEntryAsync("cache", "key", CancellationToken.None);

        _ = await Assert.That(insertEntryCalls).IsEqualTo(1);
        _ = await Assert.That(getEntryCalls).IsEqualTo(1);
        _ = await Assert.That(result).IsSameReferenceAs(entry);
    }

    /// <summary>Ensures value reads route through the decorated pipeline.</summary>
    [Test]
    public async Task GetValueUsesDecoratedPipelineAsync()
    {
        var getValueCalls = 0;
        var decorated = new ISquirixServerEntryCachePipelineCreateExpectations<object?>();
        _ = decorated.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                     .Callback((_, _, _) =>
                      {
                          getValueCalls++;
                          return ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));
                      });
        var adapter = new ExtensionCachePipelineAdapter<object?>(new ILogicalNamespacedCacheCreateExpectations<object?>().Instance(), decorated.Instance());

        _ = await adapter.GetValueAsync("cache", "key", CancellationToken.None);

        _ = await Assert.That(getValueCalls).IsEqualTo(1);
    }

    /// <summary>Ensures a decorating pipeline sees written structured values as elements and the core keeps the same bytes.</summary>
    [Test]
    public async Task WritesShowElementsToExtension()
    {
        const string text = """{ "id": 7,  "tags": ["a"] }""";
        var seen = new List<object?>();
        var decorated = new ISquirixServerEntryCachePipelineCreateExpectations<object?>();
        _ = decorated.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                     .Callback((_, _, _, written, _) =>
                      {
                          seen.Add(written.Value);
                          return ValueTask.CompletedTask;
                      });
        _ = decorated.Setups.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                     .Callback((_, _, _, written, _) =>
                      {
                          seen.Add(written.Value);
                          return ValueTask.FromResult(true);
                      });
        _ = decorated.Setups.UpdateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<object?>(), Arg.Any<CancellationToken>())
                     .Callback((_, _, _, written, _) =>
                      {
                          seen.Add(written);
                          return ValueTask.FromResult(true);
                      });
        var adapter = new ExtensionCachePipelineAdapter<object?>(new ILogicalNamespacedCacheCreateExpectations<object?>().Instance(), decorated.Instance());
        var entry = new NodeCacheEntry<object?> { Value = JsonValueKit.Create(text), Version = 1 };

        await adapter.SetEntryAsync(UnitMutationOpIds.Default, "cache", "key", entry, CancellationToken.None);
        _ = await adapter.TryAddEntryAsync(UnitMutationOpIds.Default, "cache", "key", entry, CancellationToken.None);
        _ = await adapter.UpdateAsync(UnitMutationOpIds.Default, "cache", "key", entry.Value, CancellationToken.None);

        _ = await Assert.That(seen.Count).IsEqualTo(3);
        await AssertElementKeepsTextAsync(seen[0], text);
        await AssertElementKeepsTextAsync(seen[1], text);
        await AssertElementKeepsTextAsync(seen[2], text);
    }

    /// <summary>Ensures reads through a decorating pipeline return the internal bytes unchanged, whitespace included.</summary>
    [Test]
    public async Task ReadsReturnSameBytesThroughExtension()
    {
        const string text = """{ "id": 7,  "tags": ["a"] }""";
        var stored = new NodeCacheEntry<object?> { Value = JsonValueKit.Create(text), Version = 3 };
        var basic = new BasicExtensionCachePipelineAdapter<object?>(CreateCoreReturning(stored));
        var adapter = new ExtensionCachePipelineAdapter<object?>(new ILogicalNamespacedCacheCreateExpectations<object?>().Instance(), basic);

        var entry = await adapter.GetEntryAsync("cache", "key", CancellationToken.None);
        var value = await adapter.GetValueAsync("cache", "key", CancellationToken.None);
        var removed = await adapter.RemoveAsync(UnitMutationOpIds.Default, "cache", "key", CancellationToken.None);

        await AssertBytesAsync(entry!.Value, text);
        await AssertBytesAsync(value.Value, text);
        await AssertBytesAsync(removed.Value, text);
    }

    private static async Task AssertElementKeepsTextAsync(object? value, string text)
    {
        var element = await Assert.That(value).IsTypeOf<JsonElement>();
        _ = await Assert.That(element.GetRawText()).IsEqualTo(text);
        await AssertBytesAsync(NodeCacheEntry<object?>.NormalizeValue(element), text);
    }

    private static async Task AssertBytesAsync(object? value, string text)
    {
        var json = await Assert.That(value).IsTypeOf<JsonUtf8Value>();
        _ = await Assert.That(Encoding.UTF8.GetString(json!.Bytes.Span)).IsEqualTo(text);
    }

    private static ILogicalNamespacedCache<object?> CreateCoreReturning(NodeCacheEntry<object?> stored)
    {
        var core = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = core.Setups.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult<NodeCacheEntry<object?>?>(stored));
        _ = core.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(new NodeCacheValueResult<object?>(true, stored.Value)));
        _ = core.Setups.RemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(new CacheRemoveResult<object?>(true, stored.Value)));
        return core.Instance();
    }
}
