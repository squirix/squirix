using System;
using System.Threading;

namespace Squirix.Internal.Cluster.Observability;

internal static class RpcDeadlineContext
{
    private static readonly AsyncLocal<DateTime?> DeadlineUtc = new();

    private static DateTime? CurrentDeadlineUtc => DeadlineUtc.Value;

    public static TimeSpan? GetRemainingBudget(DateTime nowUtc)
    {
        var deadline = CurrentDeadlineUtc;
        return deadline is null ? null : deadline.Value - nowUtc;
    }
}
