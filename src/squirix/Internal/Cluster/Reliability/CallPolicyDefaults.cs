using System;

namespace Squirix.Internal.Cluster.Reliability;

/// <summary>Default retry and timeout budgets for the public SDK remote cache client pool.</summary>
/// <remarks>
/// Bootstrap channel connect uses <see cref="Transport.BootstrapConnectOptions" /> because TLS/handshake
/// and endpoint probing need a longer budget. Cache RPCs share the same per-attempt budget as the
/// server cluster internode call policy.
/// </remarks>
internal static class CallPolicyDefaults
{
    /// <summary>Maximum number of transport-level retry attempts per RPC.</summary>
    private const int MaxAttempts = 3;

    /// <summary>Initial retry backoff before jitter is applied.</summary>
    private static readonly TimeSpan BaseBackoff = TimeSpan.FromMilliseconds(60);

    /// <summary>Upper bound for retry backoff before jitter is applied.</summary>
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromMilliseconds(600);

    /// <summary>Per-attempt timeout for remote cache RPCs issued by the public <c language="csharp">SquirixClient</c>.</summary>
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Gets the absolute deadline shared by one client operation across endpoint failover, stale-term reroute and transport retries.</summary>
    /// <remarks>
    /// The deadline is <see cref="PerAttemptTimeout" /> multiplied by <see cref="MaxAttempts" /> plus two, so one fully hung
    /// endpoint consumes at most the retry budget of its own attempts and still leaves at least one full attempt for the next endpoint.
    /// </remarks>
    internal static TimeSpan OperationDeadline => PerAttemptTimeout * (MaxAttempts + 2);

    internal static CallPolicy Create(string peer) => new(PerAttemptTimeout, MaxAttempts, BaseBackoff, MaxBackoff, peer: peer);
}
