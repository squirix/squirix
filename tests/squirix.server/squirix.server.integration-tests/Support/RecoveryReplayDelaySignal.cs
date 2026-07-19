using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Test-only gate that delays durable cache replay until released.</summary>
internal sealed class RecoveryReplayDelaySignal
{
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal ValueTask WaitAsync(CancellationToken cancellationToken) => new(_release.Task.WaitAsync(cancellationToken));

    internal void Release() => _release.TrySetResult();
}
