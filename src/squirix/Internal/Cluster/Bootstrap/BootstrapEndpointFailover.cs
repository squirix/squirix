using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Internal.Cluster.Bootstrap;

/// <summary>
/// Routes single-node remote cache calls across bootstrap endpoints, failing over on transport-level errors.
/// </summary>
internal sealed class BootstrapEndpointFailover
{
    private readonly Lock _activeIndexGate = new();
    private readonly string[] _bootstrapNodeIds;
    private int _activeIndex;

    public BootstrapEndpointFailover(string[] bootstrapNodeIds, string primaryNodeId)
    {
        ArgumentNullException.ThrowIfNull(bootstrapNodeIds);
        if (bootstrapNodeIds.Length == 0)
            throw new ArgumentException("At least one bootstrap node id is required.", nameof(bootstrapNodeIds));

        _bootstrapNodeIds = bootstrapNodeIds;
        _activeIndex = ResolveActiveIndex(bootstrapNodeIds, primaryNodeId);
    }

    public ValueTask<TResult> ExecuteAsync<TResult>(Func<string, CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
        ExecuteAsync(static (nodeId, callback, token) => callback(nodeId, token), action, cancellationToken);

    public async ValueTask<TResult> ExecuteAsync<TState, TResult>(Func<string, TState, CancellationToken, ValueTask<TResult>> action, TState state, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        var startIndex = ActiveIndexSnapshot();
        Exception? lastFailure = null;

        for (var attempt = 0; attempt < _bootstrapNodeIds.Length; attempt++)
        {
            var nodeIndex = (startIndex + attempt) % _bootstrapNodeIds.Length;
            var nodeId = _bootstrapNodeIds[nodeIndex];

            try
            {
                var result = await action(nodeId, state, cancellationToken).ConfigureAwait(false);
                if (nodeIndex != startIndex)
                    SetActiveIndex(nodeIndex);

                return result;
            }
            catch (Exception ex) when (BootstrapFailoverClassifier.IsFailoverEligible(ex) && attempt < _bootstrapNodeIds.Length - 1)
            {
                lastFailure = ex;
            }
        }

        throw lastFailure ?? new InvalidOperationException("Bootstrap endpoint failover failed without a captured exception.");
    }

    private static int ResolveActiveIndex(string[] bootstrapNodeIds, string primaryNodeId)
    {
        for (var i = 0; i < bootstrapNodeIds.Length; i++)
        {
            if (string.Equals(bootstrapNodeIds[i], primaryNodeId, StringComparison.Ordinal))
                return i;
        }

        throw new InvalidOperationException($"Bootstrap primary node '{primaryNodeId}' is not configured.");
    }

    private int ActiveIndexSnapshot()
    {
        lock (_activeIndexGate)
            return _activeIndex;
    }

    private void SetActiveIndex(int nodeIndex)
    {
        lock (_activeIndexGate)
            _activeIndex = nodeIndex;
    }
}
