using System;

namespace Squirix.Server.Timing;

internal interface IClock
{
    DateTime UtcNow { get; }
}
