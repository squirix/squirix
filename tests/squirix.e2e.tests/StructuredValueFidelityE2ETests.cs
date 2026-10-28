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
public sealed class StructuredValueFidelityE2ETests : EndToEndTestBase
{
    private const decimal PreciseDecimal = 12345678901234567.123456789m;
    private const string BigNumber = "12345678901234567890";
    private const string SpacedObject = """{"id": 1,"id": 2, "n":1.2345678901234567890123,"nested":{"a":[1, 2 ,3]}}""";

    /// <summary>A decimal keeps every digit on the node and after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DecimalKeepsEveryDigit(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(DecimalKeepsEveryDigit), persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<decimal>("fidelity-decimal", cancellationToken: cancellationToken);
        await cache.SetAsync("k", PreciseDecimal, cancellationToken: cancellationToken);
        var before = await cache.GetValueAsync("k", cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restarted = await cluster.GetCacheAsync<decimal>("fidelity-decimal", cancellationToken: cancellationToken);
        var after = await restarted.GetValueAsync("k", cancellationToken);

        _ = await Assert.That(before.Value).IsEqualTo(PreciseDecimal);
        _ = await Assert.That(after.Value).IsEqualTo(PreciseDecimal);
    }

    /// <summary>A number beyond a long and a double, and an object with white space and duplicate names, read back as the exact text sent, also after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ElementTextSurvivesRestart(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(ElementTextSurvivesRestart), persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<JsonElement>("fidelity-element", cancellationToken: cancellationToken);
        await cache.SetAsync("number", JsonElement.Parse(BigNumber), cancellationToken: cancellationToken);
        await cache.SetAsync("object", JsonElement.Parse(SpacedObject), cancellationToken: cancellationToken);
        var numberBefore = (await cache.GetEntryAsync("number", cancellationToken)).Value.GetRawText();
        var objectBefore = (await cache.GetEntryAsync("object", cancellationToken)).Value.GetRawText();

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restarted = await cluster.GetCacheAsync<JsonElement>("fidelity-element", cancellationToken: cancellationToken);
        var numberAfter = (await restarted.GetEntryAsync("number", cancellationToken)).Value.GetRawText();
        var objectAfter = (await restarted.GetValueAsync("object", cancellationToken)).Value.GetRawText();

        _ = await Assert.That(numberBefore).IsEqualTo(BigNumber);
        _ = await Assert.That(objectBefore).IsEqualTo(SpacedObject);
        _ = await Assert.That(numberAfter).IsEqualTo(BigNumber);
        _ = await Assert.That(objectAfter).IsEqualTo(SpacedObject);
    }

    /// <summary>A custom record is equal on the node and after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordIsEqualBeforeAndAfterRestart(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(RecordIsEqualBeforeAndAfterRestart), persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<TypedMutableCart>("fidelity-record", cancellationToken: cancellationToken);
        var expected = TypedValueFactory.CreateCart("fidelity");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        var before = await cache.GetValueAsync("k", cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restarted = await cluster.GetCacheAsync<TypedMutableCart>("fidelity-record", cancellationToken: cancellationToken);
        var after = await restarted.GetValueAsync("k", cancellationToken);

        await TypedValueAssertions.AssertCartEqualsAsync(expected, before.Value!);
        await TypedValueAssertions.AssertCartEqualsAsync(expected, after.Value!);
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
}
