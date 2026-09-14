using System;
using System.Threading;
using Squirix.Attributes;

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

    private static DateTime? Normalize(DateTime? date) => date == null || date == DateTime.MaxValue || date == DateTime.MinValue ? null : date.Value.ToUniversalTime();

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
