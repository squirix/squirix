using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.TestKit.Diagnostics;

/// <summary>Best-effort logging for exceptions intentionally suppressed in test helpers.</summary>
internal static class TestLog
{
    private static readonly Action<ILogger, string, Exception> LogSuppressed = LoggerMessage.Define<string>(LogLevel.Debug, new EventId(1, "TestSuppressed"), "{Context}");

    public static void Suppressed(string context, Exception exception) => LogSuppressed(ConsoleErrorLogger.Instance, context, exception);

    private sealed class ConsoleErrorLogger : ILogger
    {
        internal static readonly ConsoleErrorLogger Instance = new();

        private ConsoleErrorLogger()
        {
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) => Console.Error.WriteLine($"[squirix-testkit] {formatter(state, exception)}{Environment.NewLine}{exception}");
    }
}
