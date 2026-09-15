using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration tests for the default ephemeral hosting mode.
/// Uses <see cref="IntegrationSingleNodeFixture" /> to share one server across all tests.
/// </summary>
public sealed class DefaultModeIntegrationTests : NodeIntegrationTestBase
{
    /// <summary>Gets the shared single-node fixture injected once per test class.</summary>
    [ClassDataSource<IntegrationSingleNodeFixture>(Shared = SharedType.PerClass)]
    public required IntegrationSingleNodeFixture Fixture { get; init; }

    private TestNodeHost Node => Fixture.Node;

    /// <summary>Ensures cache operations work in the default ephemeral mode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultModeSupportsCacheOperations(CancellationToken cancellationToken)
    {
        var cache = GetCache(Node);

        await cache.SetEntryAsync(IntegrationMutationOpIds.Default, ServerCacheNames.DefaultNamespace, "ephemeral:key", BuildEntry("value"), cancellationToken);
        var value = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, "ephemeral:key", cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value).IsEqualTo("value");
    }

    /// <summary>Ensures default startup does not create journal, manifest, or snapshot files.</summary>
    [Test]
    public async Task DefaultStartupCreatesNoPersistedFiles()
    {
        _ = await Assert.That(Node.PersistenceEnabled).IsFalse();
        _ = await Assert.That(string.IsNullOrWhiteSpace(Node.DataDir)).IsTrue();
        _ = await Assert.That(Node.Services.GetService(typeof(PersistenceOptions))).IsNull();
    }
}
