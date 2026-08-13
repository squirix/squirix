using System;

namespace Squirix.Internal.Cluster.Reliability;

/// <summary>Default retry and timeout budgets for the public SDK remote cache client pool.</summary>
/// <remarks>
/// Bootstrap channel connect uses <see cref="Transport.BootstrapConnectOptions" /> because TLS/handshake
/// and endpoint probing need a longer budget. Cache RPCs share the same per-attempt budget as the
/// server cluster inter-node call policy.
/// </remarks>
internal static class CallPolicyDefaults
{
    /// <summary>Maximum number of transport-level retry attempts per RPC.</summary>
    private const int MaxAttempts = 3;

    /// <summary>Initial retry backoff before jitter is applied.</summary>
    private static readonly TimeSpan BaseBackoff = TimeSpan.FromMilliseconds(60);

    /// <summary>Upper bound for retry backoff before jitter is applied.</summary>
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromMilliseconds(600);

    /// <summary>
    /// Per-attempt timeout for remote cache RPCs issued by the public <c>SquirixClient</c>.
    /// </summary>
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(3);

    internal static CallPolicy Create(string peer) => new(PerAttemptTimeout, MaxAttempts, BaseBackoff, MaxBackoff, peer: peer);
}
