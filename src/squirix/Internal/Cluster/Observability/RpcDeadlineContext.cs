using System;
using System.Threading;

namespace Squirix.Internal.Cluster.Observability;

internal static class RpcDeadlineContext
{
    private static readonly AsyncLocal<Scope?> Current = new();

    /// <summary>Gets the ambient absolute operation deadline in UTC, or <see langword="null" /> when none is set.</summary>
    internal static DateTime? CurrentDeadlineUtc => Current.Value?.Deadline;

    internal static TimeSpan? GetRemainingBudget(DateTime nowUtc)
    {
        var deadline = CurrentDeadlineUtc;
        return deadline == null ? null : deadline.Value - nowUtc;
    }

    internal static IDisposable Push(DateTime? deadlineUtc)
    {
        var scope = new Scope(Normalize(deadlineUtc), Current.Value);
        Current.Value = scope;
        return scope;
    }

    private static DateTime? Normalize(DateTime? date) => date == null || date == DateTime.MaxValue || date == DateTime.MinValue ? null : date.Value.ToUniversalTime();

    /// <summary>One pushed deadline, chained to the scope that was current before it.</summary>
    /// <remarks>
    /// The ambient value is the scope itself, so a dispose acts only when this very scope is current: a repeated dispose, or the
    /// dispose of an outer scope while an inner one is current, changes nothing. When it does act, it restores the nearest enclosing
    /// scope that is still live, so a scope disposed out of order is never brought back.
    /// </remarks>
    private sealed class Scope : IDisposable
    {
        private readonly Scope? _parent;
        private int _disposed;

        internal Scope(DateTime? deadline, Scope? parent)
        {
            Deadline = deadline;
            _parent = parent;
        }

        internal DateTime? Deadline { get; }

        private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0 || !ReferenceEquals(Current.Value, this))
                return;

            var restored = _parent;
            while (restored?.IsDisposed == true)
                restored = restored._parent;

            Current.Value = restored;
        }
    }
}
