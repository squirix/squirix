using System;
using System.Threading;

namespace Squirix.Internal.Cluster.Observability;

internal static class RpcDeadlineContext
{
    private static readonly AsyncLocal<Scope?> Current = new();

    /// <summary>Gets the ambient absolute operation deadline in UTC, or <see langword="null" /> when none is set.</summary>
    internal static DateTime? CurrentDeadlineUtc => Current.Value?.Deadline;

    /// <summary>Gets the absolute deadline to hand to gRPC now, rebuilt from the remaining budget; <see langword="null" /> when none is set.</summary>
    internal static DateTime? ForwardDeadlineUtc => Current.Value?.Budget?.ForwardDeadlineUtc;

    /// <summary>Gets the clock the current deadline counts down on, or <see langword="null" /> when no deadline is set.</summary>
    internal static TimeProvider? CurrentClock => Current.Value?.Budget?.Clock;

    /// <summary>Returns the budget left before the current deadline, measured on monotonic time since it was pushed.</summary>
    /// <returns>The remaining budget, negative once the deadline passed; <see langword="null" /> when no deadline is set.</returns>
    internal static TimeSpan? GetRemainingBudget() => Current.Value?.Budget?.Remaining;

    /// <summary>Pushes an absolute deadline and converts it once into a budget that counts down on monotonic time.</summary>
    /// <param name="deadlineUtc">The absolute deadline, or <see langword="null" /> for none.</param>
    /// <param name="clock">
    /// The clock the deadline is expressed in: its wall time converts the deadline into a budget, and its monotonic timestamp counts the
    /// budget down, so a wall-clock step after the push leaves the budget unchanged.
    /// </param>
    /// <returns>The scope that restores the previous deadline when disposed.</returns>
    internal static IDisposable Push(DateTime? deadlineUtc, TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(clock);
        var deadline = Normalize(deadlineUtc);
        var scope = new Scope(deadline, deadline is { } value ? DeadlineBudget.Start(value, clock) : null, Current.Value);
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

        internal Scope(DateTime? deadline, DeadlineBudget? budget, Scope? parent)
        {
            Deadline = deadline;
            Budget = budget;
            _parent = parent;
        }

        internal DeadlineBudget? Budget { get; }

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
