using System;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.UnitTests;

internal static class SingleNodeClientPool
{
    internal static IClientPool Create(Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>> transport, CallPolicy policy)
    {
#pragma warning disable CA2000 // The mock pool delegates disposal to this client.
        var client = new BinaryCacheClient(new Uri("https://localhost"), null, null, transport);
#pragma warning restore CA2000
        var poolExpectations = IClientPool.Mock(MockBehavior.Strict);
        _ = poolExpectations.ForNode(Arg.Any<string>()).Returns(new NodeClient(client, policy));
        _ = poolExpectations.BeginDrain();
        _ = poolExpectations.DisposeAsync().ReturnsAsync(client.DisposeAsync);
        return poolExpectations.Object;
    }
}
