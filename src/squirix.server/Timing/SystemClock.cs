using System;

namespace Squirix.Server.Timing;

internal sealed class SystemClock : IClock
{
    public static readonly SystemClock Instance = new();

    private SystemClock()
    {
    }

    public DateTime UtcNow => DateTime.UtcNow;
}
