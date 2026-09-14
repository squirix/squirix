using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>Optional timeout/backoff overrides for a server call policy.</summary>
/// <param name="TimeoutPerAttempt">Per-attempt request timeout override; null uses the 600 ms default.</param>
/// <param name="BaseBackoff">Base retry backoff override; null uses the 50 ms default.</param>
/// <param name="MaxBackoff">Maximum retry backoff override; null uses the 500 ms default.</param>
[Immutable]
internal sealed record CallPolicyTimeouts(TimeSpan? TimeoutPerAttempt = null, TimeSpan? BaseBackoff = null, TimeSpan? MaxBackoff = null);
