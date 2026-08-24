using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Covers gRPC mapping of journal capacity through the shared domain-error interceptor.</summary>
[Immutable]
public sealed class DomainErrorInterceptorTests : ServerUnitTestBase
{
    /// <summary>Unary handler maps journal capacity to ResourceExhausted.</summary>
    [Fact]
    public async Task UnaryMapsQuotaToResourceExhausted()
    {
        var interceptor = new ResourceExhaustedExceptionInterceptor();
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            interceptor.UnaryServerHandler("request", new TestServerCallContext(), static (_, _) => Task.FromException<string>(new JournalCapacityExceededException())));

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Equal(JournalCapacityExceededException.StableDetail, ex.Status.Detail);
    }
}
