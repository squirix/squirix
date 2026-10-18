using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Internal.Cluster.Reliability;

internal interface ICallPolicy : IAsyncDisposable
{
    void BeginDrain();

    /// <summary>Runs the action under the retry policy of the endpoint.</summary>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="action">The endpoint call to attempt.</param>
    /// <param name="state">The state passed to the action.</param>
    /// <param name="connectTimeoutEndsAttempts">
    /// Whether a connect that timed out ends the call as unavailable instead of being retried on this endpoint; set when another endpoint
    /// remains to take the call.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The action result.</returns>
    ValueTask<T> ExecuteAsync<TState, T>(Func<TState, CancellationToken, ValueTask<T>> action, TState state, bool connectTimeoutEndsAttempts, CancellationToken cancellationToken);
}
