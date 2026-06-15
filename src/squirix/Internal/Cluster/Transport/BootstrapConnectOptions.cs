using System;
using System.Runtime.InteropServices;

namespace Squirix.Internal.Cluster.Transport;

[StructLayout(LayoutKind.Auto)]
internal readonly struct BootstrapConnectOptions
{
    public static readonly TimeSpan DefaultOverallDeadline = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan DefaultPerAttemptTimeout = TimeSpan.FromSeconds(5);

    public static readonly BootstrapConnectOptions SecondaryPeerAfterPrimary = new(TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(2));

    public BootstrapConnectOptions(TimeSpan perAttemptTimeout, TimeSpan overallDeadline, TimeSpan? baseBackoff = null, TimeSpan? maxBackoff = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(perAttemptTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(overallDeadline, TimeSpan.Zero);

        PerAttemptTimeout = perAttemptTimeout;
        OverallDeadline = overallDeadline;
        BaseBackoff = baseBackoff ?? TimeSpan.FromMilliseconds(200);
        MaxBackoff = maxBackoff ?? TimeSpan.FromSeconds(2);
    }

    public TimeSpan BaseBackoff { get; }

    public TimeSpan MaxBackoff { get; }

    public TimeSpan OverallDeadline { get; }

    public TimeSpan PerAttemptTimeout { get; }
}
