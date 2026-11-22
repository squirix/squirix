using System;

namespace Squirix.Server.Logging;

/// <summary>Associates a component type with the logger selected by its node.</summary>
/// <typeparam name="T">Component category.</typeparam>
internal sealed class CategoryLogger<T> : IServerLogger<T>
{
    private readonly IServerLogger _sink;

    internal CategoryLogger(IServerLogger sink)
    {
        _sink = sink;
    }

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => _sink.BeginScope(state);

    public bool IsEnabled(ServerLogLevel logLevel) => _sink.IsEnabled(logLevel);

    public void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        _sink.Log(logLevel, eventId, state, exception, formatter);
}
