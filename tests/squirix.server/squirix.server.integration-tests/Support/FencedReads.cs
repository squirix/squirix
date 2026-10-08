using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Reads of a node under quorum reads, through its replicated cache over its local chain, fenced by a read index.</summary>
internal static class FencedReads
{
    /// <summary>Creates the replicated cache of a node over its local chain, with its reads fenced by a read index.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The fenced cache.</returns>
    internal static ReplicatedCache Fenced(ITestNodeHost host) =>
        new(
            host.Services.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey),
            host.GetRequiredService<ReplicaGroupCommitters>(),
            true);

    /// <summary>Reads a key through the fenced cache of a node with authority, retrying while the read is refused as unavailable.</summary>
    /// <param name="ledger">The election safety record, checked on every attempt.</param>
    /// <param name="host">The node with authority.</param>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">A key of the group the ledger watches.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The text of the value read, or <see langword="null" /> when the key is absent.</returns>
    internal static async Task<string?> ReadAsync(GroupAuthorityLedger ledger, ITestNodeHost host, string cacheName, string key, CancellationToken cancellationToken)
    {
        var read = new StrongBox<string?>();
        await ledger.UntilValueAsync(
            (Cache: Fenced(host), CacheName: cacheName, Key: key, Read: read),
            static async (state, token) =>
            {
                try
                {
                    state.Read.Value = (await state.Cache.GetEntryAsync(state.CacheName, state.Key, token))?.Value as string;
                    return true;
                }
                catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable)
                {
                    return false;
                }
            },
            "a fenced read is served",
            cancellationToken);
        return read.Value;
    }
}
