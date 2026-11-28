using System;

namespace Squirix.Server.Logging;

/// <summary>Writes events and correlation scopes for one node category.</summary>
internal interface IServerLogger
{
    bool IsScopeEnabled => true;

    IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    bool IsEnabled(ServerLogLevel logLevel);

    void LogWithScope<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter, object? scope) =>
        Log(logLevel, eventId, state, exception, formatter);

    void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter);
}
