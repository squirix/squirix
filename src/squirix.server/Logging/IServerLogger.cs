using System;

namespace Squirix.Server.Logging;

/// <summary>Writes events and correlation scopes for one node category.</summary>
internal interface IServerLogger
{
    bool IsScopeEnabled => true;

    IDisposable? BeginScope<TState>(TState state)
        where TState : notnull;

    bool IsEnabled(ServerLogLevel logLevel);

    void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter);
}
