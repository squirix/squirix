using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Support.Cluster;

namespace Squirix.E2ETests.Support.Stress;

/// <summary>
/// Base class for SDK stress tests. Lives outside <c>Squirix.E2ETests.Cache</c> so it may use extra infrastructure
/// helpers without widening the cache-test surface, while still exercising only the public SDK.
/// </summary>
public abstract class StressTestBase : EndToEndTestBase
{
    internal static async Task<IReadOnlyList<ISquirixClient>> ConnectClientsAsync(HostedCluster cluster, int count, string nodeId, CancellationToken cancellationToken)
    {
        var clients = new List<ISquirixClient>(count);
        for (var i = 0; i < count; i++)
            clients.Add(await cluster.ConnectClientAsync(nodeId, cancellationToken));

        return clients;
    }

    /// <summary>
    /// Creates a hard-deadline token linked to the test cancellation token so a stalled run fails fast
    /// instead of consuming the scheduled job budget.
    /// </summary>
    /// <param name="profile">The active workload profile providing the budget.</param>
    /// <returns>A linked cancellation token source that cancels after the profile budget.</returns>
    internal static CancellationTokenSource CreateDeadline(StressLoadProfile profile)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(DefaultCancellationToken);
        cts.CancelAfter(profile.Budget);
        return cts;
    }

    internal static Task RunWritersAsync(int writers, Func<int, Task> writerBody, TimeSpan budget)
    {
        ArgumentNullException.ThrowIfNull(writerBody);
        var tasks = new Task[writers];
        for (var w = 0; w < writers; w++)
            tasks[w] = writerBody(w);

        return Task.WhenAll(tasks).WaitAsync(budget, TimeProvider.System, DefaultCancellationToken);
    }
}
