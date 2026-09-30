using System;

namespace Squirix.Server.Threading;

/// <summary>Runs a callback whose failure must not escape to the caller and hands that failure back instead.</summary>
/// <remarks>
/// The single place that catches every exception from an isolated callback, so call sites stay free of broad catch blocks. It does not report the
/// failure itself: the caller decides whether to log, trace or forward it.
/// </remarks>
internal static class Isolated
{
    /// <summary>Invokes <paramref name="action" /> with <paramref name="state" /> and returns the exception it threw, if any.</summary>
    /// <typeparam name="TState">The type of the state handed to the callback.</typeparam>
    /// <param name="state">The state passed to <paramref name="action" />; a value tuple keeps the call free of allocations.</param>
    /// <param name="action">The callback, expected to be a <see langword="static" /> lambda or method group so the call does not allocate a closure.</param>
    /// <returns>The exception <paramref name="action" /> threw, or <see langword="null" /> when it completed.</returns>
    internal static Exception? Run<TState>(TState state, Action<TState> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        try
        {
            action(state);
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }
}
