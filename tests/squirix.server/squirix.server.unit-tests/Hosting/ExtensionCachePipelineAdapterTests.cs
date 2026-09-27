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
}
