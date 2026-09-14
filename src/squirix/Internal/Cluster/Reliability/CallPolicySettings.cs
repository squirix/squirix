using System;
using Squirix.Attributes;

namespace Squirix.Internal.Cluster.Reliability;

[Immutable]
internal sealed record CallPolicySettings(string Peer, int MaxAttempts, TimeSpan TimeoutPerAttempt, TimeSpan BaseBackoff, TimeSpan MaxBackoff);
