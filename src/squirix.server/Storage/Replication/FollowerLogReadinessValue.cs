using System;

namespace Squirix.Server.Storage.Replication;

/// <summary>Maps <see cref="FollowerLogReadiness" /> to and from the raw value a follower log publishes through a volatile barrier.</summary>
internal static class FollowerLogReadinessValue
{
    /// <summary>The raw value of <see cref="FollowerLogReadiness.Unknown" />, which a log starts with before startup validation.</summary>
    internal const int Unknown = 0;

    private const int FailedValue = 2;
    private const int ReadyValue = 1;

    /// <summary>Converts a raw published value to the readiness it stands for.</summary>
    /// <param name="value">The raw value.</param>
    /// <returns>The readiness; <see cref="FollowerLogReadiness.Unknown" /> for a value that stands for none.</returns>
    internal static FollowerLogReadiness FromValue(int value)
    {
        return value switch
        {
            ReadyValue => FollowerLogReadiness.Ready,
            FailedValue => FollowerLogReadiness.Failed,
            _ => FollowerLogReadiness.Unknown,
        };
    }

    /// <summary>Converts a readiness to the raw value published for it.</summary>
    /// <param name="readiness">The readiness.</param>
    /// <returns>The raw value.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="readiness" /> is not a defined readiness.</exception>
    internal static int ToValue(FollowerLogReadiness readiness)
    {
        return readiness switch
        {
            FollowerLogReadiness.Unknown => Unknown,
            FollowerLogReadiness.Ready => ReadyValue,
            FollowerLogReadiness.Failed => FailedValue,
            _ => throw new ArgumentOutOfRangeException(nameof(readiness), readiness, "Unsupported enum value."),
        };
    }
}
