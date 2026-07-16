using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Node.App;

internal sealed record DurableMutationPipeline<TContext, TAppendState, TApplyState, TResult>
{
    internal DurableMutationPipeline(
        TContext context,
        TAppendState appendState,
        Func<TContext, TAppendState, CancellationToken, ValueTask> appendJournal,
        TApplyState applyState,
        Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> applyMemory)
    {
        Context = context;
        AppendState = appendState;
        AppendJournal = appendJournal;
        ApplyState = applyState;
        ApplyMemory = applyMemory;
    }

    internal Func<TContext, TAppendState, CancellationToken, ValueTask> AppendJournal { get; }

    internal TAppendState AppendState { get; }

    internal Func<TContext, TApplyState, CancellationToken, ValueTask<TResult>> ApplyMemory { get; }

    internal TApplyState ApplyState { get; }

    internal TContext Context { get; }
}
