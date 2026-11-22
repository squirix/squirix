using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Composition;

internal interface INodeService
{
    Task StartAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);
}
