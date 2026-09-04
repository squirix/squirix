using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Shared single-node fixture for integration test classes.
/// Starts one <see cref="TestNodeHost"/> in <see cref="InitializeAsync"/> and disposes it in <see cref="DisposeAsync"/>.
/// </summary>
/// <remarks>
/// <para><b>CRITICAL: Cache isolation.</b> All tests sharing this fixture see the same in-memory cache.
/// Every test MUST use unique cache keys to prevent cross-test interference.
/// If a test writes a key that another test reads or asserts on, the suite becomes order-dependent.
/// </para>
/// </remarks>
public sealed class IntegrationSingleNodeFixture : NodeIntegrationTestBase, IAsyncLifetime
{
    private TestNodeHost? _node;

    /// <summary>Gets the started test node host.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the fixture has not been initialized.</exception>
    public TestNodeHost Node => ThrowHelper.Required(_node, "Fixture is not initialized.");

    /// <summary>Gets the listen URI of the started node.</summary>
    public Uri Uri => Node.Uri;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_node != null)
            await _node.DisposeAsync();
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        var uri = GetNextHttpUri();
        _node = await StartNodeAsync(uri, "node-a");
    }
}
