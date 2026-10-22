using System;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Grpc;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Defaults for the per-owner call policy that guards forwarded cache calls.</summary>
/// <remarks>
/// A forwarded call is sent once: the caller retries with its own deadline and operation id, so an entry node never
/// multiplies a failed forward into more internode traffic. The per-owner permit is a bulkhead sized from the
/// entry admission limit, so a hung owner pins at most that many entry slots. With a single attempt the backoff
/// settings are never used.
/// </remarks>
internal static class ForwardingCallPolicyDefaults
{
    private const int MaxAttempts = 1;

    private static readonly CallPolicyTimeouts Timeouts = new(RpcAttemptTimeout.PerAttempt, TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(600));

    /// <summary>Gets the longest time one forwarded call may take before it is canceled.</summary>
    internal static TimeSpan TimeoutPerAttempt { get; } = RpcAttemptTimeout.PerAttempt;

    /// <summary>Gets the per-owner forwarding permit count for an entry admission limit: half of it, and at least one.</summary>
    /// <param name="maxInFlight">Entry node maximum in-flight operations.</param>
    /// <returns>The maximum number of concurrent forwarded calls to one owner.</returns>
    internal static int MaxConcurrentPerPeer(int maxInFlight) => Math.Max(1, maxInFlight / 2);

    /// <summary>Creates the call policy for forwarding to one owner.</summary>
    /// <param name="instrumentation">Call-policy instrumentation.</param>
    /// <param name="peer">Owner node id used as the metric label.</param>
    /// <param name="maxInFlight">Entry node maximum in-flight operations.</param>
    /// <param name="timeProvider">The time source of the call timeouts.</param>
    /// <returns>A single-attempt, non-queuing call policy.</returns>
    internal static ServerCallPolicy Create(ServerCallPolicyInstrumentation instrumentation, string peer, int maxInFlight, TimeProvider timeProvider) =>
        new(instrumentation, timeProvider, MaxAttempts, MaxConcurrentPerPeer(maxInFlight), peer, Timeouts);

    /// <summary>Checks the forward connect timeout against the time a forward is guaranteed to have left.</summary>
    /// <param name="connectTimeout">The configured forward connect timeout.</param>
    /// <returns>
    /// <see langword="null" /> when the timeout is at least <see cref="BoundedDial.MinTimeout" /> and below
    /// <see cref="ElectionTimerOptions.LeaderRefusalMargin" />; otherwise the error. A wait for a leader ends at least that margin before the
    /// attempt or call deadline, so a forward can start with only the margin left; a connect that outlasts it ends as the deadline, which is
    /// ambiguous, instead of as a connect failure another member may take over.
    /// </returns>
    internal static string? ValidateConnectTimeout(TimeSpan connectTimeout) =>
        connectTimeout >= BoundedDial.MinTimeout && connectTimeout < ElectionTimerOptions.LeaderRefusalMargin
            ? null
            : $"ForwardConnectTimeout must be at least {BoundedDial.MinTimeout:c} and below the {ElectionTimerOptions.LeaderRefusalMargin:c} leader refusal margin, the time a forward is guaranteed to have left (for example \"00:00:00.500\").";
}
