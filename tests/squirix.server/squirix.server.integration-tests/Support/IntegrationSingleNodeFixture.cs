using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Utils;
using TUnit.Core.Interfaces;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Shared single-node fixture for integration test classes.
/// Starts one <see cref="TestNodeHost" /> in <see cref="InitializeAsync" /> and disposes it in <see cref="DisposeAsync" />.
/// </summary>
/// <remarks>
///     <para>
///     <b>CRITICAL: Cache isolation.</b> All tests sharing this fixture see the same in-memory cache.
///     Every test MUST use unique cache keys to prevent cross-test interference.
///     If a test writes a key that another test reads or asserts on, the suite becomes order-dependent.
///     </para>
/// </remarks>
[UsedImplicitly]
public sealed class IntegrationSingleNodeFixture : NodeIntegrationTestBase, IAsyncInitializer, IAsyncDisposable
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
    public async Task InitializeAsync() => _node = await StartNodeAsync(GetNextHttpUri(), "node-a");
}
