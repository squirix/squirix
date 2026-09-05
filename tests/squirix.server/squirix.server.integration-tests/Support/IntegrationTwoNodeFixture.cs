using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Shared two-node fixture for integration test classes.
/// Starts two <see cref="TestNodeHost" /> instances in <see cref="InitializeAsync" /> and disposes them in <see cref="DisposeAsync" />.
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
public sealed class IntegrationTwoNodeFixture : NodeIntegrationTestBase, IAsyncLifetime
{
    private TestNodeHost? _nodeA;
    private TestNodeHost? _nodeB;

    /// <summary>Gets the listen URI of the first node.</summary>
    public Uri UriA => NodeA.Uri;

    /// <summary>Gets the listen URI of the second node.</summary>
    public Uri UriB => NodeB.Uri;

    /// <summary>Gets the first node host.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the fixture has not been initialized.</exception>
    private TestNodeHost NodeA => ThrowHelper.Required(_nodeA, "Fixture is not initialized.");

    /// <summary>Gets the second node host.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the fixture has not been initialized.</exception>
    private TestNodeHost NodeB => ThrowHelper.Required(_nodeB, "Fixture is not initialized.");

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_nodeB != null)
            await _nodeB.DisposeAsync();
        if (_nodeA != null)
            await _nodeA.DisposeAsync();
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        _nodeA = await StartNodeAsync(uriA, peers);
        _nodeB = await StartNodeAsync(uriB, peers);
    }
}
