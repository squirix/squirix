using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Allocation-free dispatcher for <see cref="IWorkPoolItem"/> items across pool threads.</summary>
/// <remarks>
/// A single cached delegate is reused for every dispatch, and the work is passed as a reference state object, so no
/// closure or boxing is allocated per call. The dispatch uses <see cref="Task.Factory"/> with
/// <see cref="TaskCreationOptions.DenyChildAttach"/> (combined with any caller-supplied options) and
/// <see cref="TaskScheduler.Default"/>.
/// </remarks>
internal static class WorkPool
{
    private static readonly Action<object?> Callback = static state =>
    {
        if (state is IWorkPoolItem work)
            work.Execute();
    };

    internal static Task RunAsync(IWorkPoolItem work, CancellationToken cancellationToken = default) =>
        Task.Factory.StartNew(Callback, work, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

    internal static Task RunAsync(IWorkPoolItem work, TaskCreationOptions options, CancellationToken cancellationToken = default) =>
        Task.Factory.StartNew(Callback, work, cancellationToken, options | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
}
