using System;
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
    private HostedCluster? _cluster;

    /// <summary>Gets the connected SDK client for the shared cluster node.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the fixture is not initialized.</exception>
    public ISquirixClient Client
    {
        get => field ?? ThrowFixtureNotInitialized();
        private set;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
            await _cluster.DisposeAsync();
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        _cluster = await HostedCluster.StartSingleNodeAsync(nameof(SingleNodeFixture), cancellationToken: DefaultCancellationToken);
        Client = await _cluster.ConnectClientAsync(cancellationToken: DefaultCancellationToken);
    }

    private static ISquirixClient ThrowFixtureNotInitialized() => throw new InvalidOperationException("Fixture is not initialized.");
}
