using System;

namespace Squirix.Server.Cluster.Reliability;

internal sealed record ServerCallPolicySettings(
    string Peer,
    int MaxAttempts,
    TimeSpan TimeoutPerAttempt,
    TimeSpan BaseBackoff,
    TimeSpan MaxBackoff);
