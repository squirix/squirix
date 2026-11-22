using System.Threading;

namespace Squirix.Server.Composition;

internal interface INodeLifetime
{
    CancellationToken ApplicationStarted { get; }

    CancellationToken ApplicationStopping { get; }

    void StopApplication();
}
