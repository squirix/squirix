using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.App;

/// <summary>Append + apply stages for a durable mutation, with a single packed state bag.</summary>
/// <typeparam name="TState">Caller-owned state passed to append and apply delegates.</typeparam>
/// <typeparam name="TResult">Mutation result type.</typeparam>
[Immutable]
internal sealed record DurableMutationPipeline<TState, TResult>
{
    internal DurableMutationPipeline(
        TState state,
        Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> appendJournal,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory,
        Func<TState, TResult, ValueTask>? afterAppend = null,
        Action<TState, TResult>? afterApply = null)
    {
        State = state;
        AppendJournal = appendJournal;
        ApplyMemory = applyMemory;
        AfterAppend = afterAppend;
        AfterApply = afterApply;
    }

    /// <summary>
    /// Gets the optional phase that runs once the journal frame is on the ring and before the wait for its flush, outside the mutation gate, with the
    /// result the precondition predicted. It runs only for a write whose precondition predicted a result and whose durable source is the cache journal.
    /// A failure it throws is reported as an unknown outcome, because the frame is already on the ring.
    /// </summary>
    internal Func<TState, TResult, ValueTask>? AfterAppend { get; }

    /// <summary>
    /// Gets the optional phase that runs right after the memory apply succeeded, with its actual result, never when the apply failed. It runs outside the
    /// mutation gate while the apply slot is still held; a failure it throws propagates to the caller.
    /// </summary>
    internal Action<TState, TResult>? AfterApply { get; }

    /// <summary>Gets the journal append stage; it runs under the mutation gate and passes the given gate ownership to its journal append.</summary>
    internal Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> AppendJournal { get; }

    internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

    internal TState State { get; }
}
