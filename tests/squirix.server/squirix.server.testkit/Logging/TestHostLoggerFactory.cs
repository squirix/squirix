using System;
using Squirix.Server.Logging;

namespace Squirix.Server.TestKit.Logging;

/// <summary>Writes debug and higher node events to the console when enabled for a test host.</summary>
internal sealed class TestHostLoggerFactory : IServerLoggerFactory
{
    private readonly bool _consoleLogging;

    internal TestHostLoggerFactory(bool consoleLogging = true)
    {
        _consoleLogging = consoleLogging;
    }

    public IServerLogger CreateLogger(string categoryName) => _consoleLogging ? new ConsoleLogger(categoryName) : NullLogger.Instance;

    public void Dispose()
    {
    }

    private sealed class ConsoleLogger : IServerLogger
    {
        private readonly string _categoryName;

        internal ConsoleLogger(string categoryName)
        {
            _categoryName = categoryName;
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(ServerLogLevel logLevel) => logLevel is >= ServerLogLevel.Debug and < ServerLogLevel.None;

        public void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                Console.WriteLine($"{logLevel}: {_categoryName}[{eventId.Id}] {formatter(state, exception)}{(exception == null ? null : $"{Environment.NewLine}{exception}")}");
        }
    }
}
