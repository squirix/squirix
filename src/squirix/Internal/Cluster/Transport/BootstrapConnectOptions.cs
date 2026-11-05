using System;
using Squirix.Attributes;

namespace Squirix.Internal.Cluster.Transport;

[Immutable]
internal sealed record BootstrapConnectOptions
{
    internal static readonly TimeSpan DefaultOverallDeadline = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultPerAttemptTimeout = TimeSpan.FromSeconds(5);
    internal static readonly BootstrapConnectOptions SecondaryPeerAfterPrimary = new(TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(2));

    internal BootstrapConnectOptions(TimeSpan perAttemptTimeout, TimeSpan overallDeadline)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(perAttemptTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(overallDeadline, TimeSpan.Zero);

        PerAttemptTimeout = perAttemptTimeout;
        OverallDeadline = overallDeadline;
    }

    internal TimeSpan BaseBackoff { get; } = TimeSpan.FromMilliseconds(200);

    internal TimeSpan MaxBackoff { get; } = TimeSpan.FromSeconds(2);

    internal TimeSpan OverallDeadline { get; }

    internal TimeSpan PerAttemptTimeout { get; }
}
