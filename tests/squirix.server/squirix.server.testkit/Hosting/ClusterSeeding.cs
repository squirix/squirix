using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Node.Services;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Waits until a started cluster has seeded the groups of its nodes, so a test starts against a cluster whose groups take writes.</summary>
/// <remarks>
/// A group of a fresh cluster takes no part until its owner ran the first start of the group, which needs every member up once. A node whose
/// host runs no group registry has no first start to wait for.
/// </remarks>
internal static class ClusterSeeding
{
    /// <summary>Bounds the wait for the first start of every group; it only needs the members to answer each other once.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    /// <summary>Waits until the owner of every group of the started nodes completed its copy of the group.</summary>
    /// <param name="nodes">The started nodes of one cluster.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once every first start ended.</returns>
    /// <exception cref="TimeoutException">A first start did not end within the bound.</exception>
    internal static async Task AwaitSeededAsync(IEnumerable<ITestNodeHost> nodes, CancellationToken cancellationToken)
    {
        var starts = new List<Task>();
        foreach (var node in nodes)
            StartsOf(node, starts);

        try
        {
            await Task.WhenAll(starts).WaitAsync(Bound, TimeProvider.System, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException($"The first start of the groups did not end within {Bound}.", exception);
        }
    }

    /// <summary>Adds the first start a node runs; a node that was shut down holds no services and adds none.</summary>
    /// <param name="node">A node of the cluster.</param>
    /// <param name="starts">The first starts collected so far.</param>
    private static void StartsOf(ITestNodeHost node, List<Task> starts)
    {
        try
        {
            foreach (var service in node.GetService<IEnumerable<IHostedService>>() ?? [])
            {
                if (service is ReplicaGroupBootstrapService { ExecuteTask: { } start })
                    starts.Add(start);
            }
        }
        catch (ObjectDisposedException)
        {
            // An abruptly shut down node stays registered in its cluster until it is started again.
        }
    }
}
