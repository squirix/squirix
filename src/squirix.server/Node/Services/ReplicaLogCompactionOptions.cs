using System;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Schedule of the owned replica group log maintenance run by <see cref="ReplicaLogCompactionService" />.</summary>
[Immutable]
internal sealed class ReplicaLogCompactionOptions
{
    private static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(10);

    /// <summary>Initializes a new instance of the <see cref="ReplicaLogCompactionOptions" /> class.</summary>
    internal ReplicaLogCompactionOptions()
    {
        Interval = DefaultInterval;
    }

    /// <summary>Gets the delay between two maintenance passes; 10 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The interval is not positive.</exception>
    internal TimeSpan Interval
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The maintenance interval must be greater than zero.");

            field = value;
        }
    }
}
