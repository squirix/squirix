using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Logger double recording the event id, level, exception and formatted message of every entry.</summary>
[ThreadSafe]
internal sealed class EventRecordingLogger : ILogger<DurableMutationExecutor>, ILogger<RpcMutationIdempotencyCoordinator>, ILogger<FollowerLog>, ILogger<Ledger>, ILogger<ReplicaGroupCommitter>, ILogger<ServerClientPool>, ILogger<RingAgreement>, ILogger<JournalEventLoop>,
    ILogger<ReplicaApplyService>, ILogger<ReplicaLogCompactionService>, ILogger<ReplicaExpirationSweepService>
{
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _awaited = new();
    private readonly ConcurrentQueue<(int EventId, LogLevel Level, Exception? Cause, string Message)> _events = new();

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        _events.Enqueue((eventId.Id, logLevel, exception, formatter(state, exception)));
        if (_awaited.TryGetValue(eventId.Id, out var awaited))
            _ = awaited.TrySetResult();
    }

    /// <summary>Returns a task that completes once an entry with <paramref name="eventId" /> is logged, or at once when one already was.</summary>
    /// <param name="eventId">Event id to wait for.</param>
    /// <returns>The task completing on the first entry with the event id.</returns>
    internal Task WhenLoggedAsync(int eventId)
    {
        var awaited = _awaited.GetOrAdd(eventId, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        if (Count(eventId) > 0)
            _ = awaited.TrySetResult();

        return awaited.Task;
    }

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

    /// <summary>Finds the formatted message of the first entry with <paramref name="eventId" />.</summary>
    /// <param name="eventId">Event id to look for.</param>
    /// <returns>The formatted message, or <see langword="null" /> when the event was not logged.</returns>
    internal string? FindMessage(int eventId)
    {
        foreach (var recorded in _events)
        {
            if (recorded.EventId == eventId)
                return recorded.Message;
        }

        return null;
    }
}
