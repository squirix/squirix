using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Snapshot scheduling and readiness share the service managed by the host.</summary>
public sealed class SnapshotServiceRegistrationTests : NodeIntegrationTestBase
{
    /// <summary>The persistent node resolves one snapshot trigger through each service registration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistenceSharesSnapshotService(CancellationToken cancellationToken)
    {
        const string nodeId = "snapshot-registration";
        await using var cluster = await StartClusterAsync(nodeId, new IntegrationStartOptions { UsePersistence = true }, cancellationToken);
        var node = cluster[nodeId];
        var trigger = node.GetRequiredService<SnapshotTriggerService>();
        var readiness = node.GetRequiredService<ISnapshotReadinessStatus>();
        var hosted = await Assert.That(node.GetServices<INodeService>()).HasSingleItem(static service => service is SnapshotTriggerService);

        _ = await Assert.That(readiness).IsSameReferenceAs(trigger);
        _ = await Assert.That(hosted).IsSameReferenceAs(trigger);
    }
}
