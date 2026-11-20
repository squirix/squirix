using System;

namespace Squirix.Server.Logging;

/// <summary>Writes events and correlation scopes for one node category.</summary>
internal interface ILogger
{
    IDisposable? BeginScope<TState>(TState state)
        where TState : notnull;

    bool IsEnabled(LogLevel logLevel);

    void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter);
}
