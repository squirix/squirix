using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Covers gRPC mapping of journal capacity through the inbound call interceptor.</summary>
[Immutable]
public sealed class DomainErrorInterceptorTests : ServerUnitTestBase
{
    /// <summary>Unary handler maps journal capacity to ResourceExhausted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnaryMapsQuotaToResourceExhausted(CancellationToken cancellationToken)
    {
        var interceptor = InboundCallInterceptors.Create();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(interceptor.UnaryServerHandler("request", new TestServerCallContext(), static (_, _) => Task.FromException<string>(new JournalCapacityExceededException())), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(JournalCapacityExceededException.StableDetail);
    }
}
