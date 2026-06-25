using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Services;

/// <summary>Coordinates replay-or-execute semantics for mutating cache RPC handlers.</summary>
internal sealed class RpcMutationIdempotencyCoordinator : IRpcMutationIdempotencyCoordinator
{
    private readonly IJournalCoordinator? _journal;
    private readonly RpcMutationIdempotencyStore _store;

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store, IJournalCoordinator journal)
        : this(store)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    public async Task<TResponse> ExecuteAsync<TState, TResponse>(
        string rawOperationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        ArgumentNullException.ThrowIfNull(execute);

        var operationId = RpcMutationContracts.RequireOperationId(rawOperationId);
        if (_guard.TryReplay(operationId, fingerprint, DefaultParser<TResponse>.Instance, out var cached))
            return cached ?? throw new InvalidOperationException("Replayed response was not cached.");

        var response = await execute(state, cancellationToken).ConfigureAwait(false);
        _guard.RecordSuccess(operationId, fingerprint, response);
        return response;
    }

    private static class DefaultParser<T>
        where T : IMessage<T>, new()
    {
        public static readonly MessageParser<T> Instance = new(static () => new T());
    }
}
