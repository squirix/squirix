using System;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Stale-term server responses reroute at most once to another endpoint.</summary>
[Immutable]
public sealed class RoutingRefreshTests : ServerUnitTestBase
{
    /// <summary>A stale term consumes the single reroute; a second stale term stops instead of bouncing.</summary>
    [Fact]
    public void StaleTermCausesAtMostOneServerReroute()
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        var budget = new RerouteBudget(deadline, TimeProvider.System);

        var first = new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm));
        Assert.Equal(StaleTermVerdict.Stale, StaleTermClassifier.Classify(first));
        Assert.True(budget.TryConsumeReroute());

        var second = new RpcException(new Status(StatusCode.FailedPrecondition, RefusalCodes.StaleTerm));
        Assert.Equal(StaleTermVerdict.Stale, StaleTermClassifier.Classify(second));
        Assert.False(budget.TryConsumeReroute());

        Assert.Equal(StaleTermVerdict.Current, StaleTermClassifier.Classify(StatusCode.FailedPrecondition, "other-detail"));
        Assert.Equal(StaleTermVerdict.Current, StaleTermClassifier.Classify(StatusCode.Unavailable, RefusalCodes.StaleTerm));
        Assert.Equal(StaleTermVerdict.Current, StaleTermClassifier.Classify(StatusCode.OK, RefusalCodes.StaleTerm));
    }
}
