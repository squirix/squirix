using System;
using System.Threading;

namespace Squirix.Server.Node.Observability;

internal static class RpcDeadlineContext
{
    private static readonly AsyncLocal<DateTime?> DeadlineUtc = new();

    private static DateTime? CurrentDeadlineUtc => DeadlineUtc.Value;

    public static DateTime? EffectiveDeadline(DateTime? existingDeadlineUtc)
    {
        var existing = Normalize(existingDeadlineUtc);
        var current = CurrentDeadlineUtc;
        var deadline = existing <= current ? existing : current;
        var time = current is null ? existing : deadline;
        return existing is null ? current : time;
    }

    public static TimeSpan? GetRemainingBudget(DateTime nowUtc)
    {
        var deadline = CurrentDeadlineUtc;
        return deadline is null ? null : deadline.Value - nowUtc;
    }

    public static IDisposable Push(DateTime? deadlineUtc)
    {
        var previous = DeadlineUtc.Value;
        DeadlineUtc.Value = Normalize(deadlineUtc);
        return new Scope(previous);
    }

    private static DateTime? Normalize(DateTime? deadlineUtc)
    {
        if (deadlineUtc is null || deadlineUtc == DateTime.MaxValue || deadlineUtc == DateTime.MinValue)
            return null;

        return deadlineUtc.Value.Kind == DateTimeKind.Utc ? deadlineUtc.Value : deadlineUtc.Value.ToUniversalTime();
    }

    private sealed class Scope : IDisposable
    {
        private readonly DateTime? _previous;

        public Scope(DateTime? previous)
        {
            _previous = previous;
        }

        public void Dispose() => DeadlineUtc.Value = _previous;
    }
}
