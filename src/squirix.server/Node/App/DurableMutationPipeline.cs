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
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
    {
        State = state;
        AppendJournal = appendJournal;
        ApplyMemory = applyMemory;
    }

    /// <summary>Gets the journal append stage; it runs under the mutation gate and passes the given gate ownership to its journal append.</summary>
    internal Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> AppendJournal { get; }

    internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

    internal TState State { get; }
}
