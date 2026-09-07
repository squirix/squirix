using System;
using System.Threading;

namespace Squirix.Internal.Cluster.Observability;

internal static class RpcDeadlineContext
{
    private static readonly AsyncLocal<DateTime?> DeadlineUtc = new();

    private static DateTime? CurrentDeadlineUtc => DeadlineUtc.Value;

    internal static TimeSpan? GetRemainingBudget(DateTime nowUtc)
    {
        var deadline = CurrentDeadlineUtc;
        return deadline == null ? null : deadline.Value - nowUtc;
    }

    internal static IDisposable Push(DateTime? deadlineUtc)
    {
        var previous = DeadlineUtc.Value;
        DeadlineUtc.Value = Normalize(deadlineUtc);
        return new Scope(previous);
    }

    private static DateTime? Normalize(DateTime? deadlineUtc)
    {
        if (deadlineUtc == null || deadlineUtc == DateTime.MaxValue || deadlineUtc == DateTime.MinValue)
            return null;

        return deadlineUtc.Value.Kind is DateTimeKind.Utc ? deadlineUtc.Value : deadlineUtc.Value.ToUniversalTime();
    }

    private sealed class Scope : IDisposable
    {
        private readonly DateTime? _previous;

        internal Scope(DateTime? previous)
        {
            _previous = previous;
        }

        public void Dispose() => DeadlineUtc.Value = _previous;
    }
}
