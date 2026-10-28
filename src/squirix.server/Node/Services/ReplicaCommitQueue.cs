using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>The writes of one replica group that wait for a commit batch, and the flag that says whether a leader is combining them.</summary>
/// <remarks>
/// <para>
/// A write that finds no leader leads and is never queued; the others queue behind it. The leader takes the commit gate, forms one batch from the queue,
/// commits it, and hands the role to the first request still queued, or clears the flag when there is none. Both the flag and the queue change under one
/// lock, so a request is either queued behind a leader that is yet to hand over, or leads itself.
/// </para>
/// <para>Only the leader reads the head and removes it; the other callers add at the tail and cancel their own queued request.</para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaCommitQueue
{
    private readonly Queue<ReplicaCommitRequest> _queue = [];
    private readonly Lock _sync = new();
    private bool _combining;

    /// <summary>Adds a request: it leads when no leader exists, otherwise it queues.</summary>
    /// <param name="request">The request.</param>
    /// <returns><see langword="true" /> when the request leads and is not queued.</returns>
    internal bool Enter(ReplicaCommitRequest request)
    {
        lock (_sync)
        {
            if (!_combining)
            {
                _combining = true;
                request.BeginLeading();
                return true;
            }

            _queue.Enqueue(request);
            return false;
        }
    }

    /// <summary>Ends the leadership: the first queued request that is not canceled leads next, or no one leads.</summary>
    /// <remarks>The promotion wakes its caller, which takes the commit gate in its own call; the flag stays set until that leader hands over.</remarks>
    internal void HandOver()
    {
        lock (_sync)
        {
            while (_queue.Count > 0)
            {
                if (_queue.Dequeue().TryPromote())
                    return;
            }

            _combining = false;
        }
    }

    /// <summary>Takes every queued request that is not canceled, for a leader that found the group disposed, and clears the leadership.</summary>
    /// <returns>The taken requests, in arrival order.</returns>
    /// <remarks>A request a leader already took is dropped from the queue without being returned: that leader decides it.</remarks>
    internal List<ReplicaCommitRequest> TakeAll()
    {
        var taken = new List<ReplicaCommitRequest>(_queue.Count);
        lock (_sync)
        {
            while (_queue.Count > 0)
            {
                var head = _queue.Dequeue();
                if (head.TryTake())
                    taken.Add(head);
            }

            _combining = false;
        }

        return taken;
    }

    /// <summary>Reads the first queued request that is not canceled, dropping the canceled ones before it.</summary>
    /// <returns>The head, or <see langword="null" /> when the queue holds no live request.</returns>
    /// <remarks>The head stays queued until <see cref="RemoveHead" />.</remarks>
    internal ReplicaCommitRequest? PeekLive()
    {
        lock (_sync)
        {
            while (_queue.Count > 0)
            {
                var head = _queue.Peek();
                if (!head.IsCanceled())
                    return head;

                _ = _queue.Dequeue();
            }

            return null;
        }
    }

    /// <summary>Removes the head, which the leader took into its batch.</summary>
    /// <param name="expected">The request the leader took.</param>
    /// <remarks>Nothing is removed when the head is another request or the queue is empty: a disposal may have taken the queued requests meanwhile.</remarks>
    internal void RemoveHead(ReplicaCommitRequest expected)
    {
        lock (_sync)
        {
            if (_queue.Count > 0 && ReferenceEquals(_queue.Peek(), expected))
                _ = _queue.Dequeue();
        }
    }

    /// <summary>Hands a request the leader took but cannot batch back to the head of the queue.</summary>
    /// <param name="request">The request, which the leader took from the head.</param>
    /// <returns>
    /// <see langword="true" /> when the request is still the head and is queued again; <see langword="false" /> when a disposal took it from the queue meanwhile, so the
    /// caller decides it.
    /// </returns>
    internal bool Restore(ReplicaCommitRequest request)
    {
        lock (_sync)
        {
            if (_queue.Count == 0 || !ReferenceEquals(_queue.Peek(), request))
                return false;

            request.Release();
            return true;
        }
    }
}
