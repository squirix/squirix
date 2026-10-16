using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Opens the forward connection from every started node to every other one, so no forward a test measures is the first one.</summary>
/// <remarks>
/// <para>
/// The first forward between two nodes opens its TLS connection. On a loaded host that handshake can outlast the per-attempt timeout of the
/// forward, which ends the forward as an ambiguous timeout instead of the answer the test expects.
/// </para>
/// <para>
/// Each node sends one read with an empty cache name to each peer over its forward channel. The peer refuses it before routing, so it waits
/// for no leader and changes no state, but the connection it opened stays in the pool. Every answer and every failure of a sent read is
/// ignored. A client that cannot send the read at all throws, and the start fails: that is a broken test double, not a slow peer.
/// </para>
/// </remarks>
internal static class ForwardChannelWarmUp
{
    /// <summary>The longest wait for one warm-up read; a peer that does not answer by then only leaves its connection cold.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>Sends one warm-up read from every node to every other node, all at once, and waits for them.</summary>
    /// <param name="nodes">The started nodes, by node identifier; a node without a client pool, such as a stand-in host, sends nothing.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once every warm-up read ended.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled, which the reads themselves do not report.</exception>
    internal static async Task WarmAsync(IReadOnlyCollection<KeyValuePair<string, ITestNodeHost>> nodes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        var started = new List<KeyValuePair<string, ITestNodeHost>>(nodes);
        var reads = new List<Task>(started.Count * started.Count);
        for (var from = 0; from < started.Count; from++)
        {
            var pool = started[from].Value.Services.GetService<IServerClientPool>();
            for (var to = 0; pool != null && to < started.Count; to++)
            {
                if (to != from)
                    reads.Add(ReadAsync(pool.ForNode(started[to].Key), cancellationToken));
            }
        }

        await Task.WhenAll(reads).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
    }

    private static async Task ReadAsync(SquirixCacheService.SquirixCacheServiceClient client, CancellationToken cancellationToken)
    {
        using var call = client.GetValueAsync(new GetValueAsyncRequest { CacheName = string.Empty, Key = "warm-up" }, deadline: DateTime.UtcNow.Add(Bound), cancellationToken: cancellationToken);
        Task response = call.ResponseAsync;
        await response.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }
}
