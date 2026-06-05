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
        return existing is null ? current : current is null ? existing : existing <= current ? existing : current;
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
        return deadlineUtc is null || deadlineUtc == DateTime.MaxValue || deadlineUtc == DateTime.MinValue ? null :
            deadlineUtc.Value.Kind == DateTimeKind.Utc ? deadlineUtc.Value : deadlineUtc.Value.ToUniversalTime();
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
