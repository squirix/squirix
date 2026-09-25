using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Logger double recording the event id, level and exception of every entry.</summary>
[ThreadSafe]
internal sealed class EventRecordingLogger : ILogger
{
    private readonly ConcurrentQueue<(int EventId, LogLevel Level, Exception? Cause)> _events = new();

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        _events.Enqueue((eventId.Id, logLevel, exception));

    /// <summary>Counts the entries with <paramref name="eventId" />.</summary>
    /// <param name="eventId">Event id to count.</param>
    /// <returns>The number of entries logged with the event id.</returns>
    internal int Count(int eventId)
    {
        var count = 0;
        foreach (var recorded in _events)
        {
            if (recorded.EventId == eventId)
                count++;
        }

        return count;
    }

    /// <summary>Finds the first entry with <paramref name="eventId" />.</summary>
    /// <param name="eventId">Event id to look for.</param>
    /// <returns>The level and exception of the entry, or <see langword="null" /> when the event was not logged.</returns>
    internal (LogLevel Level, Exception? Cause)? Find(int eventId)
    {
        foreach (var recorded in _events)
        {
            if (recorded.EventId == eventId)
                return (recorded.Level, recorded.Cause);
        }

        return null;
    }
}
