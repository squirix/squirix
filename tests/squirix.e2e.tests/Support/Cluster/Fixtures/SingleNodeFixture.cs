using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.E2ETests.Support.Cluster.Fixtures;

/// <summary>Shared single-node cluster and SDK client for one public API test class.</summary>
[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Instantiated by xUnit via IClassFixture<T>.")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Test fixture surface must be public for xUnit class fixtures.")]
public sealed class SingleNodeFixture : NodeFixtureBase, IAsyncLifetime
{
    private HostedCluster? _cluster;
    private ISquirixClient? _client;

    /// <summary>Gets the connected SDK client for the shared cluster node.</summary>
    public ISquirixClient Client => _client!;

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartSingleNodeAsync(nameof(SingleNodeFixture), cancellationToken: DefaultCancellationToken);
        _client = await _cluster.ConnectClientAsync(cancellationToken: DefaultCancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster is not null)
            await _cluster.DisposeAsync();
    }
}
