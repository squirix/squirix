using System;
using Squirix.Server.Logging;

namespace Squirix.Server.TestKit.Logging;

internal sealed class NullLogger<T> : IServerLogger<T>
{
    internal static readonly NullLogger<T> Instance = new();

    private NullLogger()
    {
    }

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(ServerLogLevel logLevel) => false;

    public void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
    }
}
