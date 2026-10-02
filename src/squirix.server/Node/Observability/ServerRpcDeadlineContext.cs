using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

internal static class ServerRpcDeadlineContext
{
    private static readonly AsyncLocal<Scope?> Current = new();

    /// <summary>Gets the clock the current deadline counts down on, or <see langword="null" /> when no deadline is set.</summary>
    internal static TimeProvider? CurrentClock => Current.Value?.Budget?.Clock;

    private static DateTime? ForwardDeadlineUtc => Current.Value?.Budget?.ForwardDeadlineUtc;

    internal static DateTime? EffectiveDeadline(DateTime? existingDeadlineUtc)
    {
        var existing = Normalize(existingDeadlineUtc);
        var current = ForwardDeadlineUtc;
        var deadline = existing <= current ? existing : current;
        var time = current == null ? existing : deadline;
        return existing == null ? current : time;
    }

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
        var scope = new Scope(deadline, deadline is { } value ? ServerDeadlineBudget.Start(value, clock) : null, Current.Value);
        Current.Value = scope;
        return scope;
    }

    private static DateTime? Normalize(DateTime? deadlineUtc) => deadlineUtc switch
    {
        null => null,
        { } value when value == DateTime.MaxValue || value == DateTime.MinValue => null,
        { Kind: DateTimeKind.Utc } value => value,
        { } value => value.ToUniversalTime(),
    };

    /// <summary>One pushed deadline, chained to the scope that was current before it.</summary>
    /// <remarks>
    /// The ambient value is the scope itself, so a dispose acts only when this very scope is current: a repeated dispose, or the
    /// dispose of an outer scope while an inner one is current, changes nothing. When it does act, it restores the nearest enclosing
    /// scope that is still live, so a scope disposed out of order is never brought back.
    /// </remarks>
    [Mutable]
    private sealed class Scope : IDisposable
    {
        private readonly Scope? _parent;
        private int _disposed;

        internal Scope(DateTime? deadline, ServerDeadlineBudget? budget, Scope? parent)
        {
            Deadline = deadline;
            Budget = budget;
            _parent = parent;
        }

        internal ServerDeadlineBudget? Budget { get; }

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
