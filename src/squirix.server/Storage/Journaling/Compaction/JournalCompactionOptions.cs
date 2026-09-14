using System;
using System.Text.Json.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Compaction;

internal sealed class JournalCompactionOptions
{
    [JsonConstructor]
    internal JournalCompactionOptions()
    {
        MinGap = TimeSpan.FromMinutes(2);
        MinTailBytes = 64 * 1024 * 1024;
        MinTailSegments = 2;
    }

    [JsonInclude]
    internal bool Enabled { get; init; } = true;

    [JsonInclude]
    internal TimeSpan MinGap
    {
        get;
        init
        {
            value.ThrowIfNegative(nameof(value), "MinGap cannot be negative.");

            field = value;
        }
    }

    [JsonInclude]
    internal long MinTailBytes
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MinTailBytes cannot be negative.");

            field = value;
        }
    }

    [JsonInclude]
    internal int MinTailSegments
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MinTailSegments cannot be negative.");

            field = value;
        }
    }
}
