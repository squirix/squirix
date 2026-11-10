using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Numbers and structured values retain their exact representation across a restart.</summary>
[Immutable]
public sealed class StructuredValueRestartE2ETests
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
}
