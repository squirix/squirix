using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

internal sealed class JournalStartupGate
{
    private readonly TaskCompletionSource _ready = CreateCompletionSource();

    public JournalStartupGate(bool isOpen = true)
    {
        if (isOpen)
            Open();
    }

    /// <summary>
    /// Gets a value indicating whether startup recovery has completed and the gate is open.
    /// </summary>
    public bool IsReady => _ready.Task.IsCompleted;

    public void Open() => _ready.TrySetResult();

    public ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        var ready = _ready.Task;
        return !cancellationToken.CanBeCanceled || ready.IsCompleted ? new ValueTask(ready) : new ValueTask(ready.WaitAsync(cancellationToken));
    }

    private static TaskCompletionSource CreateCompletionSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
