using System;
using System.Threading.Tasks;
using Rocks;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.UnitTests;

internal static class SingleNodeClientPool
{
    internal static IClientPool Create(Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>> transport, CallPolicy policy)
    {
#pragma warning disable CA2000 // The mock pool delegates disposal to this client.
        var client = new BinaryCacheClient(new Uri("https://localhost"), null, null, transport);
#pragma warning restore CA2000
        var poolExpectations = new IClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(new NodeClient(client, policy));
        _ = poolExpectations.Setups.BeginDrain();
        _ = poolExpectations.Setups.DisposeAsync().Callback(client.DisposeAsync);
        return poolExpectations.Instance();
    }
}
