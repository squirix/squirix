using System;

namespace Squirix.Internal.Cluster.Reliability;

internal sealed record CallPolicySettings(
    string Peer,
    int MaxAttempts,
    TimeSpan TimeoutPerAttempt,
    TimeSpan BaseBackoff,
    TimeSpan MaxBackoff);
