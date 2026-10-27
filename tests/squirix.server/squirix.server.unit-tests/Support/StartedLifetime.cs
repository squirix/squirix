using System.Threading;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>An application lifetime whose started signal the test raises.</summary>
[ThreadSafe]
internal sealed class StartedLifetime : IHostApplicationLifetime
{
    internal StartedLifetime(CancellationToken started)
    {
        ApplicationStarted = started;
    }

    public CancellationToken ApplicationStarted { get; }

    public CancellationToken ApplicationStopping => CancellationToken.None;

    public CancellationToken ApplicationStopped => CancellationToken.None;

    public void StopApplication()
    {
    }
}
