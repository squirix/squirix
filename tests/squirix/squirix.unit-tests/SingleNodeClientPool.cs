using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.UnitTests;

internal static class SingleNodeClientPool
{
    internal static IClientPool Create(CallInvoker transport, CallPolicy policy)
    {
        var client = new SquirixCacheService.SquirixCacheServiceClient(transport);
        var poolExpectations = new IClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(new NodeClient(client, policy));
        _ = poolExpectations.Setups.BeginDrain();
        _ = poolExpectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        return poolExpectations.Instance();
    }
}
