using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Services;

/// <summary>Defers journal durability until idempotency outcome frames are appended for the active RPC.</summary>
internal sealed class RpcMutationIdempotencyExecutionScope : IDisposable
{
    private static readonly AsyncLocal<RpcMutationIdempotencyExecutionScope?> ActiveScope = new();

    private readonly Func<object, CancellationToken, ValueTask> _completeAsync;

    private RpcMutationIdempotencyExecutionScope(Func<object, CancellationToken, ValueTask> completeAsync)
    {
        _completeAsync = completeAsync ?? throw new ArgumentNullException(nameof(completeAsync));
    }

    public static RpcMutationIdempotencyExecutionScope? Current => ActiveScope.Value;

    public static RpcMutationIdempotencyExecutionScope Begin<TResponse>(
        RpcMutationIdempotencyStore store,
        string operationId,
        string fingerprint,
        IJournalCoordinator journal,
        Func<TResponse, byte[]> serializeResponse)
        where TResponse : IMessage<TResponse>
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(serializeResponse);

        var scope = new RpcMutationIdempotencyExecutionScope(CompleteAsync);
        ActiveScope.Value = scope;
        return scope;

        ValueTask CompleteAsync(object response, CancellationToken cancellationToken)
        {
            if (response is not TResponse typedResponse)
                throw new InvalidOperationException("Idempotency response type does not match the active execution scope.");

            var responseBytes = serializeResponse(typedResponse);
            store.RecordSuccess(operationId, fingerprint, responseBytes);
            return journal.AppendIdempotencyOutcomeAsync(operationId, fingerprint, responseBytes, cancellationToken);
        }
    }

    public ValueTask CompleteBeforeDurabilityAsync<TResponse>(TResponse response, CancellationToken cancellationToken)
        where TResponse : IMessage<TResponse>
    {
        ArgumentNullException.ThrowIfNull(response);
        return _completeAsync(response, cancellationToken);
    }

    public void Dispose()
    {
        if (ReferenceEquals(ActiveScope.Value, this))
            ActiveScope.Value = null;
    }
}
