using System;

namespace Squirix.Server.TestKit;

/// <summary>
/// Builds delegates from explicit state, so a delegate that is stored and run later reaches its state through a parameter instead of capturing a
/// local. The factory is static, so the delegates it returns close over its parameter alone: one closure for every delegate it returns, as a
/// capturing lambda costs.
/// </summary>
public static class Bind
{
    /// <summary>Creates a delegate, or a tuple of delegates sharing one closure, from explicit state.</summary>
    /// <typeparam name="TState">The state type; a value tuple carries several values.</typeparam>
    /// <typeparam name="TResult">The delegate, or the tuple of delegates, the factory returns.</typeparam>
    /// <param name="state">The state passed to the factory.</param>
    /// <param name="create">The factory, static so that it captures nothing.</param>
    /// <returns>What <paramref name="create" /> returns for <paramref name="state" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="create" /> is <see langword="null" />.</exception>
    public static TResult To<TState, TResult>(TState state, Func<TState, TResult> create)
    {
        ArgumentNullException.ThrowIfNull(create);
        return create(state);
    }
}
