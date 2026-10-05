using System;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Defaults for the per-owner call policy that guards forwarded cache calls.</summary>
/// <remarks>
/// A forwarded call is sent once: the caller retries with its own deadline and operation id, so an entry node never
/// multiplies a failed forward into more internode traffic. The per-owner permit is a bulkhead sized from the
/// entry admission limit, so a hung owner pins at most that many entry slots.
/// </remarks>
internal static class ForwardingCallPolicyDefaults
{
    private const int MaxAttempts = 1;

    private static readonly CallPolicyTimeouts Timeouts = new(TimeSpan.FromSeconds(3), TimeSpan.FromMilliseconds(60), TimeSpan.FromMilliseconds(600));

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
}
