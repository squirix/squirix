using System;

namespace Squirix.Server.Logging;

/// <summary>Associates a component type with the logger selected by its node.</summary>
/// <typeparam name="T">Component category.</typeparam>
internal sealed class CategoryLogger<T> : ILogger<T>
{
    private readonly ILogger _sink;

    internal CategoryLogger(ILogger sink)
    {
        _sink = sink;
    }

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => _sink.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => _sink.IsEnabled(logLevel);

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        _sink.Log(logLevel, eventId, state, exception, formatter);
}
