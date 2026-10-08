using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage that the client operation deadline reaches the server and is shared by forwarded calls.</summary>
public sealed class OperationDeadlineE2ETests : EndToEndTestBase
{
    private static readonly TimeSpan OperationDeadline = TimeSpan.FromSeconds(15);

    /// <summary>
    /// A call forwarded to the owner node carries a budget no larger than the forwarding node observed, up to the rounding of the wire
    /// timeout: the gRPC client rounds the timeout it sends up to three significant figures of milliseconds (14963 ms is sent as 15 s).
    /// The forwarding node still cancels the call at its own deadline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedCallSharesClientDeadline(CancellationToken cancellationToken)
    {
        var probes = CreateProbes("nodeA", "nodeB");
        var options = new MultiNodeStartOptions { ServicesConfigure = probes.Configure };
        await using var cluster = await HostedCluster.StartTwoNodeAsync(options, nameof(ForwardedCallSharesClientDeadline), cancellationToken: cancellationToken);
        var key = KeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "nodeB", "deadline-forward");

        var cache = await cluster.GetCacheAsync<string>("default", "nodeA", cancellationToken);
        await cache.SetAsync(key, "value", cancellationToken: cancellationToken);

        var atEntry = Last(probes["nodeA"]);
        var atOwner = Last(probes["nodeB"]);
        _ = await Assert.That(atEntry).IsNotNull();
        _ = await Assert.That(atOwner).IsNotNull();
        _ = await Assert.That(atOwner.GetValueOrDefault() > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(atOwner.GetValueOrDefault() <= WireTimeoutCeiling(atEntry.GetValueOrDefault())).IsTrue();
        _ = await Assert.That(atEntry.GetValueOrDefault() <= OperationDeadline).IsTrue();
    }

    /// <summary>The server observes a positive remaining budget bounded by the client operation deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ServerReceivesClientDeadline(CancellationToken cancellationToken)
    {
        var probes = CreateProbes("nodeA");
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(ServerReceivesClientDeadline),
            timeProvider: TimeProvider.System,
            configure: probes.Configure,
            cancellationToken: cancellationToken);

        var cache = await cluster.GetCacheAsync<string>("default", "nodeA", cancellationToken);
        await cache.SetAsync("deadline-key", "value", cancellationToken: cancellationToken);

        var budget = Last(probes["nodeA"]);
        _ = await Assert.That(budget).IsNotNull();
        _ = await Assert.That(budget.GetValueOrDefault() > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(budget.GetValueOrDefault() <= OperationDeadline).IsTrue();
    }

    private static ProbeSet CreateProbes(params string[] nodeIds)
    {
        var probes = new Dictionary<string, DeadlineBudgetProbe>(StringComparer.Ordinal);
        for (var i = 0; i < nodeIds.Length; i++)
            probes[nodeIds[i]] = new DeadlineBudgetProbe();

        return new ProbeSet(probes);
    }

    private static TimeSpan? Last(DeadlineBudgetProbe probe)
    {
        var budgets = probe.Snapshot();
        return budgets.Length == 0 ? null : budgets[^1];
    }

    /// <summary>Returns <paramref name="budget" /> as the gRPC client sends it: whole milliseconds rounded up to three significant figures.</summary>
    /// <param name="budget">The budget.</param>
    /// <returns>The timeout the next hop receives.</returns>
    private static TimeSpan WireTimeoutCeiling(TimeSpan budget)
    {
        var milliseconds = Convert.ToInt64(Math.Ceiling(budget.TotalMilliseconds));
        var unit = 1L;
        while (milliseconds / unit >= 1000)
            unit *= 10;

        return TimeSpan.FromMilliseconds((milliseconds + unit - 1) / unit * unit);
    }

    private sealed class ProbeSet
    {
        private readonly FrozenDictionary<string, DeadlineBudgetProbe> _probes;

        internal ProbeSet(Dictionary<string, DeadlineBudgetProbe> probes)
        {
            _probes = probes.ToFrozenDictionary(StringComparer.Ordinal);
        }

        internal DeadlineBudgetProbe this[string nodeId] => _probes[nodeId];

        internal void Configure(string nodeId, IServiceCollection services) => _probes[nodeId].Register(services);
    }
}
