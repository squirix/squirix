using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Stale-term server responses reroute at most once to another endpoint.</summary>
[Immutable]
public sealed class RoutingRefreshTests : ServerUnitTestBase
{
    /// <summary>A stale term consumes the single reroute; a second stale term stops instead of bouncing.</summary>
    [Test]
    public async Task StaleTermCausesAtMostOneServerReroute()
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        var budget = new RerouteBudget(deadline, TimeProvider.System);

        var first = new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm));
        _ = await Assert.That(StaleTermClassifier.Classify(first)).IsEqualTo(StaleTermVerdict.Stale);
        _ = await Assert.That(budget.TryConsumeReroute()).IsTrue();

        var second = new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm));
        _ = await Assert.That(StaleTermClassifier.Classify(second)).IsEqualTo(StaleTermVerdict.Stale);
        _ = await Assert.That(budget.TryConsumeReroute()).IsFalse();

        _ = await Assert.That(StaleTermClassifier.Classify(StatusCode.FailedPrecondition, "other-detail")).IsEqualTo(StaleTermVerdict.Current);
        _ = await Assert.That(StaleTermClassifier.Classify(StatusCode.Unavailable, RefusalCodes.StaleTerm)).IsEqualTo(StaleTermVerdict.Current);
        _ = await Assert.That(StaleTermClassifier.Classify(StatusCode.OK, RefusalCodes.StaleTerm)).IsEqualTo(StaleTermVerdict.Current);
    }
}
