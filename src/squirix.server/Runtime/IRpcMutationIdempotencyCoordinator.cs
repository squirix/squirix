using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Squirix.Server.Runtime;

/// <summary>Replay-or-execute coordinator for mutating cache RPC handlers.</summary>
internal interface IRpcMutationIdempotencyCoordinator
{
    /// <summary>Replays a cached outcome or executes the handler once for the given operation identifier.</summary>
    /// <typeparam name="TState">Opaque handler state passed to <paramref name="execute" />.</typeparam>
    /// <typeparam name="TResponse">Protobuf response type for the mutating RPC.</typeparam>
    /// <param name="rawOperationId">Operation identifier from the transport request.</param>
    /// <param name="fingerprint">Deterministic request fingerprint for reuse detection.</param>
    /// <param name="state">Handler state forwarded to <paramref name="execute" />.</param>
    /// <param name="execute">Handler body invoked when no cached outcome exists.</param>
    /// <param name="cancellationToken">Cancellation token for the RPC.</param>
    /// <returns>The replayed or freshly executed response.</returns>
    Task<TResponse> ExecuteAsync<TState, TResponse>(
        string rawOperationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new();
}
