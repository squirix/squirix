using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Squirix.Server.TestKit.XUnit;

/// <summary>
/// Test helpers for xUnit-based suites.
/// </summary>
public static class XUnitKit
{
    /// <summary>
    /// XUnit logger provider that pipes log messages into test output.
    /// </summary>
    public sealed class XUnitLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, ILogger> _loggers = new();
        private readonly ITestOutputHelper _output;

        /// <summary>
        /// Initializes a new instance of the <see cref="XUnitLoggerProvider" /> class.
        /// </summary>
        /// <param name="output">
        /// The test output.
        /// </param>
        public XUnitLoggerProvider(ITestOutputHelper output)
        {
            _output = output;
        }

        /// <inheritdoc />
        public ILogger CreateLogger(string categoryName) => _loggers.GetOrAdd(categoryName, name => new XUnitLogger(_output, name));

        /// <inheritdoc />
        public void Dispose()
        {
        }

        private sealed class NullStub : IDisposable
        {
            public static readonly NullStub Instance = new();

            public void Dispose()
            {
            }
        }

        private sealed class XUnitLogger : ILogger
        {
            private readonly string _name;
            private readonly ITestOutputHelper _output;

            public XUnitLogger(ITestOutputHelper output, string name)
            {
                _output = output;
                _name = name;
            }

            public IDisposable BeginScope<TState>(TState state)
                where TState : notnull => NullStub.Instance;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                try
                {
                    _output.WriteLine($"[{logLevel}] {_name}: {formatter(state, exception)}");
                    if (exception is not null)
                        _output.WriteLine(exception.ToString());
                }
                catch
                {
                    // Swallow logging errors in test environments
                }
            }
        }
    }
}
