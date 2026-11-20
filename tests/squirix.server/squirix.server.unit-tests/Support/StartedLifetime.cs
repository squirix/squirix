using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Composition;

namespace Squirix.Server.UnitTests.Support;

/// <summary>An application lifetime whose started signal the test raises.</summary>
[ThreadSafe]
internal sealed class StartedLifetime : INodeLifetime
{
    internal StartedLifetime(CancellationToken started)
    {
        ApplicationStarted = started;
    }

    public CancellationToken ApplicationStarted { get; }

    public CancellationToken ApplicationStopping => CancellationToken.None;

    public void StopApplication()
    {
    }
}
