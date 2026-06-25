using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Cluster;

namespace Squirix.E2ETests;

/// <summary>
/// Base class for SDK stress tests. Lives outside <c>Squirix.E2ETests.Cache</c> so it may use extra infrastructure
/// helpers without widening the cache-test surface, while still exercising only the public SDK.
/// </summary>
public abstract class StressTestBase : EndToEndTestBase
{
    /// <summary>Runs concurrent writer tasks until they complete or the budget elapses.</summary>
    /// <param name="writers">Number of concurrent writer tasks.</param>
    /// <param name="writerBody">Per-writer body keyed by writer index.</param>
    /// <param name="budget">Maximum time allowed for all writers.</param>
    /// <returns>A task that completes when all writers finish or the budget is exceeded.</returns>
    protected static Task RunWritersAsync(int writers, Func<int, Task> writerBody, TimeSpan budget)
    {
        ArgumentNullException.ThrowIfNull(writerBody);
        var tasks = new Task[writers];
        for (var w = 0; w < writers; w++)
            tasks[w] = writerBody(w);

        return Task.WhenAll(tasks).WaitAsync(budget, TimeProvider.System, DefaultCancellationToken);
    }

    private protected static async Task<IReadOnlyList<ISquirixClient>> ConnectClientsAsync(HostedCluster cluster, int count, string nodeId, CancellationToken cancellationToken)
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
    private protected static CancellationTokenSource CreateDeadline(LoadProfile profile)
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
