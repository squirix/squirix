using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using Squirix.E2ETests.Fixtures.TypedValues;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Numbers and structured values keep their exact digits and text on a node, after a restart, and across the kinds of cache that read them.</summary>
[Immutable]
public sealed class StructuredValueFidelityE2ETests
{
    private const decimal PreciseDecimal = 12345678901234567.123456789m;
    private const string BigNumber = "12345678901234567890";
    private const string SpacedObject = """{"id": 1,"id": 2, "n":1.2345678901234567890123,"nested":{"a":[1, 2 ,3]}}""";

    /// <summary>Decimals and JSON elements retain their exact values before and after restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExactValuesSurviveRestart(CancellationToken cancellationToken)
    {
        const string decimalCacheName = "fidelity-decimal";
        const string elementCacheName = "fidelity-element";
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(ExactValuesSurviveRestart), persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var decimals = await cluster.GetCacheAsync<decimal>(decimalCacheName, cancellationToken: cancellationToken);
        var elements = await cluster.GetCacheAsync<JsonElement>(elementCacheName, cancellationToken: cancellationToken);
        await decimals.SetAsync("k", PreciseDecimal, cancellationToken: cancellationToken);
        await elements.SetAsync("number", JsonElement.Parse(BigNumber), cancellationToken: cancellationToken);
        await elements.SetAsync("object", JsonElement.Parse(SpacedObject), cancellationToken: cancellationToken);
        _ = await Assert.That((await decimals.GetValueAsync("k", cancellationToken)).Value).IsEqualTo(PreciseDecimal);
        _ = await Assert.That((await elements.GetEntryAsync("number", cancellationToken)).Value.GetRawText()).IsEqualTo(BigNumber);
        _ = await Assert.That((await elements.GetEntryAsync("object", cancellationToken)).Value.GetRawText()).IsEqualTo(SpacedObject);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);

        var restartedDecimals = await cluster.GetCacheAsync<decimal>(decimalCacheName, cancellationToken: cancellationToken);
        var restartedElements = await cluster.GetCacheAsync<JsonElement>(elementCacheName, cancellationToken: cancellationToken);
        _ = await Assert.That((await restartedDecimals.GetValueAsync("k", cancellationToken)).Value).IsEqualTo(PreciseDecimal);
        _ = await Assert.That((await restartedElements.GetEntryAsync("number", cancellationToken)).Value.GetRawText()).IsEqualTo(BigNumber);
        _ = await Assert.That((await restartedElements.GetValueAsync("object", cancellationToken)).Value.GetRawText()).IsEqualTo(SpacedObject);
    }

    /// <summary>An untyped cache reads an int as a long and a number element as an element, and every other scalar as itself.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UntypedCacheReadsDocumentedKinds(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(UntypedCacheReadsDocumentedKinds), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<object>("fidelity-object", cancellationToken: cancellationToken);
        await cache.SetAsync("int", 5, cancellationToken: cancellationToken);
        await cache.SetAsync("long", long.MaxValue, cancellationToken: cancellationToken);
        await cache.SetAsync("double", 2.5d, cancellationToken: cancellationToken);
        await cache.SetAsync("string", "text", cancellationToken: cancellationToken);
        await cache.SetAsync("bool", true, cancellationToken: cancellationToken);
        await cache.SetAsync("number", JsonElement.Parse(BigNumber), cancellationToken: cancellationToken);
        await cache.SetAsync("object", JsonElement.Parse(SpacedObject), cancellationToken: cancellationToken);

        _ = await Assert.That((await cache.GetValueAsync("int", cancellationToken)).Value).IsTypeOf<long>();
        _ = await Assert.That((await cache.GetValueAsync("long", cancellationToken)).Value).IsEqualTo(long.MaxValue);
        _ = await Assert.That((await cache.GetValueAsync("double", cancellationToken)).Value).IsEqualTo(2.5d);
        _ = await Assert.That((await cache.GetValueAsync("string", cancellationToken)).Value).IsEqualTo("text");
        _ = await Assert.That((await cache.GetValueAsync("bool", cancellationToken)).Value is true).IsTrue();
        var number = await Assert.That((await cache.GetValueAsync("number", cancellationToken)).Value).IsTypeOf<JsonElement>();
        var structured = await Assert.That((await cache.GetValueAsync("object", cancellationToken)).Value).IsTypeOf<JsonElement>();
        _ = await Assert.That(number.GetRawText()).IsEqualTo(BigNumber);
        _ = await Assert.That(structured.GetRawText()).IsEqualTo(SpacedObject);
    }

    /// <summary>A remove answers whether it removed, and the value is gone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAnswersWithoutTheValue(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(RemoveAnswersWithoutTheValue), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<TypedMutableCart>("fidelity-remove", cancellationToken: cancellationToken);
        await cache.SetAsync("k", TypedValueFactory.CreateCart("removed"), cancellationToken: cancellationToken);

        var first = await cache.RemoveAsync("k", cancellationToken);
        var second = await cache.RemoveAsync("k", cancellationToken);

        _ = await Assert.That(first).IsTrue();
        _ = await Assert.That(second).IsFalse();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>A get-or-add that adds returns the instance the factory created, and one over a present key returns the stored value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddReturnsCreatedInstanceWhenAdded(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(GetOrAddReturnsCreatedInstanceWhenAdded), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<TypedMutableCart>("fidelity-get-or-add", cancellationToken: cancellationToken);
        var created = TypedValueFactory.CreateCart("created");

        var added = await cache.GetOrAddAsync("k", (_, _) => Task.FromResult<TypedMutableCart?>(created), cancellationToken: cancellationToken);
        var existing = await cache.GetOrAddAsync("k", static (_, _) => Task.FromResult<TypedMutableCart?>(TypedValueFactory.CreateCart("other")), cancellationToken: cancellationToken);

        _ = await Assert.That(added.Value).IsSameReferenceAs(created);
        await TypedValueAssertions.AssertCartEqualsAsync(created, existing.Value!);
    }
}
