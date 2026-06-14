using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Infrastructure;

namespace Squirix.E2ETests.PublicApi.SingleNode;

/// <summary>
/// Shared fixtures for single-node v0.1 public <see cref="ICache{T}" /> integration tests.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
public abstract class PublicApiSingleNodeTestBase : E2ETestBase
{
    internal static readonly TimeSpan Delay60 = TimeSpan.FromMilliseconds(60);
    internal static readonly TimeSpan Delay90 = TimeSpan.FromMilliseconds(90);

    internal static async ValueTask<ISquirixClient> ConnectClientAsync([CallerMemberName] string testName = "")
    {
        var cluster = await E2ECluster.StartSingleNodeAsync(testName, cancellationToken: DefaultCancellationToken);

        try
        {
            var client = await cluster.ConnectClientAsync(cancellationToken: DefaultCancellationToken);
            return new NodeHostedClient(cluster, client);
        }
        catch (InvalidOperationException)
        {
            await cluster.DisposeAsync();
            throw;
        }
        catch (IOException)
        {
            await cluster.DisposeAsync();
            throw;
        }
        catch (RpcException)
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    private sealed class NodeHostedClient : ISquirixClient
    {
        private readonly E2EClientHandle _client;
        private readonly E2ECluster _cluster;
        private int _disposed;

        public NodeHostedClient(E2ECluster cluster, E2EClientHandle client)
        {
            _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            await _client.DisposeAsync();
            await _cluster.DisposeAsync();
        }

        public ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken = default) => _client.GetCacheAsync<T>(cacheName, cancellationToken);
    }
}
