using System;
using Squirix.Server.Logging;
using HostLogging = Microsoft.Extensions.Logging;

namespace Squirix.Server.TestKit.Logging;

/// <summary>Forwards node events to the providers configured by a test host.</summary>
internal sealed class TestHostLoggerFactory : ILoggerFactory
{
    private readonly HostLogging.ILoggerFactory _factory;
    private readonly bool _ownsFactory;

    internal TestHostLoggerFactory(HostLogging.ILoggerFactory factory, bool ownsFactory = false)
    {
        _factory = factory;
        _ownsFactory = ownsFactory;
    }

    public ILogger CreateLogger(string categoryName) => new Sink(_factory.CreateLogger(categoryName));

    public void Dispose()
    {
        if (_ownsFactory)
            _factory.Dispose();
    }

    private sealed class Sink : ILogger
    {
        private readonly HostLogging.ILogger _target;

        internal Sink(HostLogging.ILogger target)
        {
            _target = target;
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => _target.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => _target.IsEnabled(ToHostLevel(logLevel));

#pragma warning disable ZA0401 // The test bridge forwards already-structured state to the selected host provider.
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var level = ToHostLevel(logLevel);
            if (_target.IsEnabled(level))
                _target.Log(level, new HostLogging.EventId(eventId.Id, eventId.Name), state, exception, formatter);
        }
#pragma warning restore ZA0401

        private static HostLogging.LogLevel ToHostLevel(LogLevel level) => level switch
        {
            LogLevel.Trace => HostLogging.LogLevel.Trace,
            LogLevel.Debug => HostLogging.LogLevel.Debug,
            LogLevel.Information => HostLogging.LogLevel.Information,
            LogLevel.Warning => HostLogging.LogLevel.Warning,
            LogLevel.Error => HostLogging.LogLevel.Error,
            LogLevel.Critical => HostLogging.LogLevel.Critical,
            LogLevel.None => HostLogging.LogLevel.None,
            _ => throw new ArgumentOutOfRangeException(nameof(level), level, "Unsupported log level."),
        };
    }
}
