using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Squirix.Server.Node.Services;

/// <summary>Coordinates replay-or-execute semantics for mutating cache RPC handlers.</summary>
internal sealed class RpcMutationIdempotencyCoordinator
{
    private readonly RpcMutationIdempotencyGuard _guard;

    public RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyGuard guard)
    {
        _guard = guard ?? throw new ArgumentNullException(nameof(guard));
    }

    public async Task<TResponse> ExecuteAsync<TState, TResponse>(
        string rawOperationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : IMessage<TResponse>, new()
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
