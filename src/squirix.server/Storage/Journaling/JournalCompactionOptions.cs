using System;

namespace Squirix.Server.Storage.Journaling;

internal sealed class JournalCompactionOptions
{
    public JournalCompactionOptions()
    {
        MinGap = TimeSpan.FromMinutes(2);
        MinTailBytes = 64 * 1024 * 1024;
        MinTailSegments = 2;
    }

    public bool Enabled { get; init; } = true;

    public TimeSpan MinGap
    {
        get;
        init
        {
            if (value < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MinGap cannot be negative.");

            field = value;
        }
    }

    public long MinTailBytes
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "MinTailBytes cannot be negative.");

            field = value;
        }
    }

    public int MinTailSegments
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
