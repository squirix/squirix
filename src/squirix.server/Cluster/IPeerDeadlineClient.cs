using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Cluster;

internal interface IPeerDeadlineClient : IPeerClient
{
    Task<T> SendWithDeadlineAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, ServerDeadlineBudget? budget, CancellationToken cancellationToken)
        where T : WireMessage;
}
