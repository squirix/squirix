using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using TUnit.Core.Interfaces;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Shared two-node fixture for integration test classes.
/// Starts a two-node <see cref="TestCluster{TOptions}" /> in <see cref="InitializeAsync" /> and disposes it in <see cref="DisposeAsync" />.
/// </summary>
/// <remarks>
///     <para>
///     <b>CRITICAL: Cache isolation.</b> All tests sharing this fixture see the same in-memory cache
///     on each node. Every test MUST use unique cache keys to prevent cross-test interference.
///     The idempotency store is also shared — operation ids must be unique across tests
///     (use <c language="csharp">RpcOperationIdentity.New()</c> per test).
///     </para>
/// </remarks>
[UsedImplicitly]
public sealed class IntegrationTwoNodeFixture : NodeIntegrationTestBase, IAsyncInitializer, IAsyncDisposable
{
    private TestCluster<IntegrationStartOptions>? _cluster;

    /// <summary>Gets the listen URI of the first node.</summary>
    public Uri UriA => Cluster["node-a"].Uri;

    /// <summary>Gets the listen URI of the second node.</summary>
    public Uri UriB => Cluster["node-b"].Uri;

    /// <summary>Gets the started two-node cluster.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the fixture has not been initialized.</exception>
    private TestCluster<IntegrationStartOptions> Cluster => ThrowHelper.Required(_cluster, "Fixture is not initialized.");

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
            await _cluster.DisposeAsync();
    }

    /// <inheritdoc />
    public async Task InitializeAsync() => _cluster = await StartClusterAsync("node-a", "node-b");
}
