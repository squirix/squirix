using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Client;
using Squirix.E2ETests.Cluster;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests;

/// <summary>Shared single-node cluster and SDK client for one public API test class.</summary>
[UsedImplicitly]
public sealed class SingleNodeFixture : NodeFixtureBase, IAsyncInitializer, IAsyncDisposable
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
    public async Task InitializeAsync()
    {
        // IAsyncInitializer has no test context to link to, so startup uses its own 30s budget.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _cluster = await HostedCluster.StartSingleNodeAsync(nameof(SingleNodeFixture), cancellationToken: cts.Token);
        Client = await _cluster.ConnectClientAsync(cancellationToken: cts.Token);
    }

    private static ISquirixClient ThrowFixtureNotInitialized() => throw new InvalidOperationException("Fixture is not initialized.");
}
