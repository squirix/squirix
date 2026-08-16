using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Test-only gate that delays durable cache replay until released.</summary>
[Immutable]
internal sealed class RecoveryReplayDelaySignal
{
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal void Release() => _release.TrySetResult();

    internal ValueTask WaitAsync(CancellationToken cancellationToken) => new(_release.Task.WaitAsync(cancellationToken));
}
