using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Node.Backpressure;

internal interface IBackpressureGate
{
    ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> AcquireAsync(string transport, string operation, string clientId, CancellationToken cancellationToken);
}
