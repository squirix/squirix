using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Squirix.Server.Adapters.Endpoint.Rest;

/// <summary>
/// In-memory ring buffer of recent admin audit events for diagnostics.
/// </summary>
internal sealed class AdminAuditSink
{
    private readonly int _capacity;
    private readonly ConcurrentQueue<AdminAuditEvent> _events = new();

    public AdminAuditSink(int capacity = 200)
    {
        _capacity = capacity;
    }

    public IReadOnlyList<AdminAuditEvent> GetSnapshot() => [.. _events];

    public void Record(AdminAuditEvent @event)
    {
        _events.Enqueue(@event);
        while (_events.Count > _capacity)
        {
            if (!_events.TryDequeue(out _))
                break;
        }
    }
}
