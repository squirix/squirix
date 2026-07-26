using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Shared single-node cluster and SDK client for one public API test class.</summary>
[UsedImplicitly]
public sealed class SingleNodeFixture : NodeFixtureBase, IAsyncLifetime
{
    private ISquirixClient? _client;
    private HostedCluster? _cluster;

    /// <summary>Gets the connected SDK client for the shared cluster node.</summary>
    public ISquirixClient Client => _client!;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster is not null)
            await _cluster.DisposeAsync();
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartSingleNodeAsync(nameof(SingleNodeFixture), cancellationToken: DefaultCancellationToken);
        _client = await _cluster.ConnectClientAsync(cancellationToken: DefaultCancellationToken);
    }
}
