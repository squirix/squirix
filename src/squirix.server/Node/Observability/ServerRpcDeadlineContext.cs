using System;
using System.Threading;
using Squirix.Attributes;

namespace Squirix.Server.Node.Observability;

internal static class ServerRpcDeadlineContext
{
    private static readonly AsyncLocal<DateTime?> DeadlineUtc = new();

    private static DateTime? CurrentDeadlineUtc => DeadlineUtc.Value;

    internal static DateTime? EffectiveDeadline(DateTime? existingDeadlineUtc)
    {
        var existing = Normalize(existingDeadlineUtc);
        var current = CurrentDeadlineUtc;
        var deadline = existing <= current ? existing : current;
        var time = current is null ? existing : deadline;
        return existing is null ? current : time;
    }

    internal static TimeSpan? GetRemainingBudget(DateTime nowUtc)
    {
        var deadline = CurrentDeadlineUtc;
        return deadline is null ? null : deadline.Value - nowUtc;
    }

    internal static IDisposable Push(DateTime? deadlineUtc)
    {
        var previous = DeadlineUtc.Value;
        DeadlineUtc.Value = Normalize(deadlineUtc);
        return new Scope(previous);
    }

    private static DateTime? Normalize(DateTime? deadlineUtc)
    {
        if (deadlineUtc is null || deadlineUtc == DateTime.MaxValue || deadlineUtc == DateTime.MinValue)
            return null;

        return deadlineUtc.Value.Kind is DateTimeKind.Utc ? deadlineUtc.Value : deadlineUtc.Value.ToUniversalTime();
    }

    [Immutable]
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
