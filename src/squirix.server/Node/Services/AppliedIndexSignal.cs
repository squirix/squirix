using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>Completes readers only when their requested applied index is reached.</summary>
/// <remarks>Registration and publication share one lock; cancellation removes a target after its last reader leaves.</remarks>
[ThreadSafe]
internal sealed class AppliedIndexSignal
{
    private readonly Lock _gate = new();
    private readonly SortedDictionary<ulong, WaitGroup> _groups = [];
    private WaitGroup? _first;
    private ulong _index;

    /// <summary>Gets the current applied index.</summary>
    internal ulong Index => Volatile.Read(ref _index);

    /// <summary>Gets the number of distinct pending target indexes.</summary>
    internal int PendingTargets
    {
        get
        {
            lock (_gate)
                return _groups.Count;
        }
    }

    /// <summary>Waits for the requested applied index, sharing a completion with readers of the same target.</summary>
    /// <param name="index">The requested index.</param>
    /// <param name="cancellationToken">Cancellation of this reader only.</param>
    /// <returns>A task completed once the target is reached.</returns>
    internal async Task WaitAsync(ulong index, CancellationToken cancellationToken)
    {
        WaitGroup group;
        lock (_gate)
        {
            if (_index >= index)
                return;

            if (!_groups.TryGetValue(index, out group!))
            {
                group = new WaitGroup(index);
                _groups.Add(index, group);
                if (_first == null || index < _first.Index)
                    _first = group;
            }

            group.Readers++;
        }

        try
        {
            await group.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
            {
                group.Readers--;
                if (group.Readers == 0 && _groups.TryGetValue(index, out var current) && ReferenceEquals(current, group))
                    Remove(group);
            }
        }
    }

    /// <summary>Publishes the current applied index, including a reset to a lower index.</summary>
    /// <param name="index">The newly applied or installed position.</param>
    internal void Notify(ulong index)
    {
        lock (_gate)
        {
            Volatile.Write(ref _index, index);
            while (_first is { } group && group.Index <= index)
            {
                Remove(group);
                _ = group.Completion.TrySetResult();
            }
        }
    }

    private void Remove(WaitGroup group)
    {
        _ = _groups.Remove(group.Index);
        if (!ReferenceEquals(_first, group))
            return;

        using var groups = _groups.GetEnumerator();
        _first = groups.MoveNext() ? groups.Current.Value : null;
    }

    [Mutable]
    private sealed class WaitGroup
    {
        internal WaitGroup(ulong index)
        {
            Index = index;
        }

        internal TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ulong Index { get; }

        internal int Readers { get; set; }
    }
}
