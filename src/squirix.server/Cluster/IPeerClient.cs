using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Cluster;

/// <summary>Sends one binary operation to a configured peer.</summary>
internal interface IPeerClient
{
    Task<T> SendAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
        where T : WireMessage;
}
