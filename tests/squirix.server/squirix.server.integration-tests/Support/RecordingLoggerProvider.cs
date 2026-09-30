using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Logger provider recording every entry a host logs, with its category.</summary>
[ThreadSafe]
internal sealed class RecordingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<RecordedLogEntry> _entries = new();

    public ILogger CreateLogger(string categoryName) => new RecordingLogger(this, categoryName);

    public void Dispose()
    {
    }

    /// <summary>Finds the first entry with <paramref name="eventId" />.</summary>
    /// <param name="eventId">Event id to look for.</param>
    /// <returns>The entry, or <see langword="null" /> when the host logger never received the event.</returns>
    internal RecordedLogEntry? Find(int eventId)
    {
        foreach (var entry in _entries)
        {
            if (entry.EventId == eventId)
                return entry;
        }

        return null;
    }

    /// <summary>Takes a snapshot of every recorded entry.</summary>
    /// <returns>The entries in recording order.</returns>
    internal IReadOnlyList<RecordedLogEntry> Snapshot() => [.. _entries];

    /// <summary>Registers this provider with the host logger factory.</summary>
    /// <param name="services">The host service collection.</param>
    internal void Register(IServiceCollection services) => _ = services.AddSingleton<ILoggerProvider>(this);

    private void Record(RecordedLogEntry entry) => _entries.Enqueue(entry);

    /// <summary>One entry a host logged.</summary>
    /// <param name="EventId">The event id.</param>
    /// <param name="Level">The log level.</param>
    /// <param name="Category">The logger category.</param>
    /// <param name="Message">The formatted message.</param>
    [Immutable]
    internal sealed record RecordedLogEntry(int EventId, LogLevel Level, string Category, string Message);

    [Immutable]
    private sealed class RecordingLogger : ILogger
    {
        private readonly string _category;
        private readonly RecordingLoggerProvider _owner;

        internal RecordingLogger(RecordingLoggerProvider owner, string category)
        {
            _owner = owner;
            _category = category;
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            _owner.Record(new RecordedLogEntry(eventId.Id, logLevel, _category, formatter(state, exception)));
    }
}
