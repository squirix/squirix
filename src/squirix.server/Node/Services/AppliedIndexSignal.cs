using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>A multi-waiter notification that the applied index of a replica group advanced.</summary>
/// <remarks>
/// The signal holds a pending completion source only while some reader asked for the next advance, so an advance with no reader
/// allocates nothing. A reader registers through <see cref="NextAsync" /> before it re-checks the index; the advancer writes the index before
/// it calls <see cref="Notify" />. Both sides use full fences (the compare-exchange and the exchange), so either the reader sees the new
/// index or the advancer sees the registered source: no wakeup is lost.
/// </remarks>
[ThreadSafe]
internal sealed class AppliedIndexSignal
{
    private TaskCompletionSource? _pending;

    /// <summary>Gets a task that completes at the next <see cref="Notify" /> after this call.</summary>
    /// <returns>The task of the pending source, shared by all readers registered before the next advance.</returns>
    [SuppressMessage("Usage", "VSTHRD003", Justification = "The signal creates the source and completes it only from Notify.")]
    internal Task NextAsync()
    {
        var pending = Volatile.Read(ref _pending);
        if (pending != null)
            return pending.Task;

        var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return (Interlocked.CompareExchange(ref _pending, created, null) ?? created).Task;
    }

    /// <summary>Wakes every reader registered since the last call.</summary>
    internal void Notify() => Interlocked.Exchange(ref _pending, null)?.TrySetResult();
}
