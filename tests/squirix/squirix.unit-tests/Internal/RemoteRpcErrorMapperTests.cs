using Grpc.Core;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Unit tests for remote RPC error mapping in the public SDK client.</summary>
public sealed class RemoteRpcErrorMapperTests
{
    /// <summary>
    /// Ensures missing operation id faults map to <see cref="OperationIdRequiredException" />.
    /// </summary>
    [Fact]
    public void MapsMissingOperationIdToTypedException()
    {
        var rpc = new RpcException(new Status(StatusCode.InvalidArgument, OperationIdRequiredException.StableDetail));
        var ex = Assert.Throws<OperationIdRequiredException>(() => { RemoteRpcErrorMapper.Map(rpc); });
        Assert.Same(rpc, ex.InnerException);
    }

    /// <summary>
    /// Ensures operation-id reuse mismatch faults map to <see cref="OperationIdReuseMismatchException" />.
    /// </summary>
    [Fact]
    public void MapsReuseMismatchToTypedException()
    {
        var rpc = new RpcException(new Status(StatusCode.FailedPrecondition, OperationIdReuseMismatchException.StableDetail));
        var ex = Assert.Throws<OperationIdReuseMismatchException>(() => { RemoteRpcErrorMapper.Map(rpc); });
        Assert.Same(rpc, ex.InnerException);
    }
}
