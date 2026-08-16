using System;
using Squirix.Attributes;

namespace Squirix.Internal.Cluster.Transport;

[Immutable]
internal sealed record BootstrapConnectOptions
{
    internal static readonly TimeSpan DefaultOverallDeadline = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultPerAttemptTimeout = TimeSpan.FromSeconds(5);
    internal static readonly BootstrapConnectOptions SecondaryPeerAfterPrimary = new(TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(2));

    internal BootstrapConnectOptions(TimeSpan perAttemptTimeout, TimeSpan overallDeadline, TimeSpan? baseBackoff = null, TimeSpan? maxBackoff = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(perAttemptTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(overallDeadline, TimeSpan.Zero);

        PerAttemptTimeout = perAttemptTimeout;
        OverallDeadline = overallDeadline;
        BaseBackoff = baseBackoff ?? TimeSpan.FromMilliseconds(200);
        MaxBackoff = maxBackoff ?? TimeSpan.FromSeconds(2);
    }

    internal TimeSpan BaseBackoff { get; }

    internal TimeSpan MaxBackoff { get; }

    internal TimeSpan OverallDeadline { get; }

    internal TimeSpan PerAttemptTimeout { get; }
}
