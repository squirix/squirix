using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Writes committed through the committers of a node with authority over a group a <see cref="GroupAuthorityLedger" /> watches.</summary>
internal static class LedgerWrites
{
    /// <summary>Commits one write through the committers of a node with authority, retrying while the group is not ready to take it.</summary>
    /// <param name="ledger">The election safety record, checked on every attempt.</param>
    /// <param name="node">The node with authority.</param>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="key">A key of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="operationId">The operation identifier, also the written value; a new one when not set.</param>
    /// <returns>A task that completes once the write committed.</returns>
    /// <remarks>Every attempt carries the same operation identifier, so a retry never writes twice.</remarks>
    internal static Task CommitAsync(GroupAuthorityLedger<IntegrationStartOptions> ledger, ITestNodeHost node, string cacheName, string key, CancellationToken cancellationToken, string? operationId = null) =>
        ledger.UntilValueAsync(
            (Committers: node.GetRequiredService<ReplicaGroupCommitters>(), CacheName: cacheName, Key: key, OperationId: operationId ?? Guid.NewGuid().ToString("N")),
            static async (write, token) =>
            {
                try
                {
                    await write.Committers.ForKey(write.CacheName, write.Key)
                        .CommitSetAsync(write.OperationId, write.CacheName, write.Key, new NodeCacheEntry<object?> { Value = write.OperationId }, token);
                    return true;
                }
                catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable or StatusCode.ResourceExhausted)
                {
                    return false;
                }
                catch (SquirixException)
                {
                    return false;
                }
            },
            "a write commits",
            cancellationToken);
}
