using System;
using Squirix.Transport.Grpc;

namespace Squirix.Internal.Cluster.Reliability;

/// <summary>Default retry and timeout budgets for the public SDK remote cache client pool.</summary>
/// <remarks>
/// Bootstrap channel connect uses <see cref="Transport.BootstrapConnectOptions" /> because TLS/handshake
/// and endpoint probing need a longer budget. Cache RPCs share the same per-attempt budget as the
/// server cluster internode call policy.
/// </remarks>
internal static class CallPolicyDefaults
{
    /// <summary>Initial retry backoff before jitter is applied.</summary>
    private static readonly TimeSpan BaseBackoff = TimeSpan.FromMilliseconds(60);

    /// <summary>Upper bound for retry backoff before jitter is applied.</summary>
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromMilliseconds(600);

    /// <summary>Per-attempt timeout for remote cache RPCs issued by the public <c language="csharp">SquirixClient</c>.</summary>
    private static readonly TimeSpan PerAttemptTimeout = RpcAttemptTimeout.PerAttempt;

    /// <summary>Gets the absolute deadline shared by one client operation across endpoint failover, stale-term reroute and transport retries.</summary>
    /// <remarks>See <see cref="RpcAttemptTimeout.OperationDeadline" />; the server sizes its idempotency window against the same value.</remarks>
    internal static TimeSpan OperationDeadline => RpcAttemptTimeout.OperationDeadline;

    internal static CallPolicy Create(string peer, TimeProvider timeProvider) => new(timeProvider, PerAttemptTimeout, RpcAttemptTimeout.MaxAttempts, BaseBackoff, MaxBackoff, peer: peer);
}
