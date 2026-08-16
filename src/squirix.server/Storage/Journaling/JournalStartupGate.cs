using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling;

[Immutable]
internal sealed class JournalStartupGate
{
    private readonly TaskCompletionSource _ready = TaskCompletionSourceFactory.Create();

    internal JournalStartupGate(bool isOpen = true)
    {
        if (isOpen)
            Open();
    }

    /// <summary>Gets a value indicating whether startup recovery has completed and the gate is open.</summary>
    internal bool IsReady => _ready.Task.IsCompleted;

    internal void Open() => _ready.TrySetResult();

    internal ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        var ready = _ready.Task;
        return !cancellationToken.CanBeCanceled || ready.IsCompleted ? new ValueTask(ready) : new ValueTask(ready.WaitAsync(cancellationToken));
    }
}
