using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness based on journal startup recovery completion.</summary>
[Immutable]
internal sealed class JournalRecoveryReadinessHealthCheck : IReadinessCheck
{
    private readonly AsyncManualResetEvent _event;

    internal JournalRecoveryReadinessHealthCheck(AsyncManualResetEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        _event = @event;
    }

    /// <inheritdoc />
    public Task<ReadinessResult> CheckHealthAsync(CancellationToken cancellationToken)
    {
        var result = _event.IsSet ? ReadinessResult.Healthy("journal recovery is complete.") : ReadinessResult.Unhealthy("journal recovery is still in progress.");
        return Task.FromResult(result);
    }
}
