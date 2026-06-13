using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2ETests.Infrastructure.Stress;

/// <summary>
/// Base class for SDK stress tests. Lives outside <c>Squirix.E2ETests.PublicApi</c> so it may use extra infrastructure
/// helpers without widening the PublicApi architecture guard, while still exercising only the public SDK surface.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
public abstract class StressE2ETestBase : E2ETestBase
{
    internal static async Task<IReadOnlyList<E2EClientHandle>> ConnectClientsAsync(E2ECluster cluster, int count, string nodeId, CancellationToken cancellationToken)
    {
        var clients = new List<E2EClientHandle>(count);
        for (var i = 0; i < count; i++)
            clients.Add(await cluster.ConnectClientAsync(nodeId, cancellationToken).ConfigureAwait(false));

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

    internal static async Task RunWritersAsync(int writers, Func<int, Task> writerBody, TimeSpan budget)
    {
        ArgumentNullException.ThrowIfNull(writerBody);
        var tasks = new Task[writers];
        for (var w = 0; w < writers; w++)
            tasks[w] = writerBody(w);

        await Task.WhenAll(tasks).WaitAsync(budget).ConfigureAwait(false);
    }
}
