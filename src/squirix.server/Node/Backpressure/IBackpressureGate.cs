using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Node.Backpressure;

internal interface IBackpressureGate
{
    ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string transport, string operation, string clientId, CancellationToken cancellationToken);
}
