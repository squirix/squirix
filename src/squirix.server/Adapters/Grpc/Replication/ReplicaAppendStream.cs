using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Owns ordered append exchanges independently for every peer and replica group.</summary>
[ThreadSafe]
internal sealed class ReplicaAppendStream : IDisposable
{
    private readonly IServerClientPool _pool;
    private readonly Dictionary<(string Node, string Group), ReplicaAppendSession> _sessions = [];
    private readonly Lock _sync = new();
    private int _disposed;

    internal ReplicaAppendStream(IServerClientPool pool)
    {
        _pool = pool;
    }

    /// <summary>Refuses new exchanges and closes every existing session; repeated disposal is harmless.</summary>
    public void Dispose()
    {
        ReplicaAppendSession[] sessions;
        lock (_sync)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            sessions = new ReplicaAppendSession[_sessions.Count];
            _sessions.Values.CopyTo(sessions, 0);
            _sessions.Clear();
        }

        foreach (var session in sessions)
            session.Dispose();
    }

    internal Task<AppendReplicaEntriesResponse> SendAsync(string node, AppendReplicaEntriesRequest request, CancellationToken cancellationToken)
    {
        ReplicaAppendSession session;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var key = (node, request.Header.GroupId);
            if (!_sessions.TryGetValue(key, out session!))
            {
                session = new ReplicaAppendSession(_pool, node);
                _sessions.Add(key, session);
            }
        }

        return session.SendAsync(request, cancellationToken);
    }
}
