using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Node.App;

/// <summary>Append + apply stages for a durable mutation, with a single packed state bag.</summary>
/// <typeparam name="TState">Caller-owned state passed to append and apply delegates.</typeparam>
/// <typeparam name="TResult">Mutation result type.</typeparam>
internal sealed record DurableMutationPipeline<TState, TResult>
{
    internal DurableMutationPipeline(
        TState state,
        Func<TState, CancellationToken, ValueTask> appendJournal,
        Func<TState, CancellationToken, ValueTask<TResult>> applyMemory)
    {
        State = state;
        AppendJournal = appendJournal;
        ApplyMemory = applyMemory;
    }

    internal Func<TState, CancellationToken, ValueTask> AppendJournal { get; }

    internal Func<TState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

    internal TState State { get; }
}
