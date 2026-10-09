using System;
using Squirix.Server.Node.Observability;

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

    private const int PerAttemptTimeoutMilliseconds = 3000;

    private static readonly CallPolicyTimeouts Timeouts = new(TimeSpan.FromMilliseconds(PerAttemptTimeoutMilliseconds), TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(600));

    /// <summary>Gets the longest time one forwarded call may take before it is canceled.</summary>
    internal static TimeSpan TimeoutPerAttempt { get; } = TimeSpan.FromMilliseconds(PerAttemptTimeoutMilliseconds);

    /// <summary>Gets the per-owner forwarding permit count for an entry admission limit: half of it, and at least one.</summary>
    /// <param name="maxInFlight">Entry node maximum in-flight operations.</param>
    /// <returns>The maximum number of concurrent forwarded calls to one owner.</returns>
    internal static int MaxConcurrentPerPeer(int maxInFlight) => Math.Max(1, maxInFlight / 2);

    /// <summary>Creates the call policy for forwarding to one owner.</summary>
    /// <param name="instrumentation">Call-policy instrumentation.</param>
    /// <param name="peer">Owner node id used as the metric label.</param>
    /// <param name="maxInFlight">Entry node maximum in-flight operations.</param>
    /// <param name="timeProvider">Optional time provider.</param>
    /// <returns>A single-attempt, non-queuing call policy.</returns>
    internal static ServerCallPolicy Create(ServerCallPolicyInstrumentation instrumentation, string peer, int maxInFlight, TimeProvider? timeProvider) =>
        new(instrumentation, MaxAttempts, MaxConcurrentPerPeer(maxInFlight), peer, timeProvider, Timeouts);

    /// <summary>Checks the internode connect timeout against the forwarded call it bounds.</summary>
    /// <param name="connectTimeout">The configured connect timeout.</param>
    /// <returns>
    /// <see langword="null" /> when the timeout is positive and below <see cref="TimeoutPerAttempt" />; otherwise the error. A connect that
    /// outlasts the attempt ends as the attempt timeout, which is ambiguous, instead of as a connect failure another member may take over.
    /// </returns>
    internal static string? ValidateConnectTimeout(TimeSpan connectTimeout) =>
        connectTimeout > TimeSpan.Zero && connectTimeout < TimeoutPerAttempt
            ? null
            : $"InterNodeConnectTimeout must be positive and below the {TimeoutPerAttempt:c} per-attempt timeout of a forwarded call (for example \"00:00:00.300\").";
}
