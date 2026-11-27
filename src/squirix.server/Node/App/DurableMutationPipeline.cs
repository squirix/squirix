using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.App;

/// <summary>Append + apply stages for a durable mutation, with a single packed state bag.</summary>
/// <typeparam name="TState">Caller-owned state passed to append and apply delegates.</typeparam>
/// <typeparam name="TResult">Mutation result type.</typeparam>
[Immutable]
internal readonly record struct DurableMutationPipeline<TState, TResult>
{
    internal DurableMutationPipeline(
        TState state,
        Func<TState, JournalAppendContext, CancellationToken, ValueTask> appendJournal,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory,
        Action<TState, TResult>? afterApply = null)
    {
        State = state;
        AppendJournal = appendJournal;
        ApplyMemory = applyMemory;
        AfterApply = afterApply;
    }

    /// <summary>
    /// Gets the optional phase that runs right after the memory apply succeeded, with its actual result, never when the apply failed. It runs outside the
    /// mutation gate while the apply slot is still held; a failure it throws propagates to the caller.
    /// </summary>
    internal Action<TState, TResult>? AfterApply { get; }

    /// <summary>Gets the journal append stage; it runs under the mutation gate and passes the given gate ownership to its journal append.</summary>
    internal Func<TState, JournalAppendContext, CancellationToken, ValueTask> AppendJournal { get; }

    internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

    internal TState State { get; }
}
