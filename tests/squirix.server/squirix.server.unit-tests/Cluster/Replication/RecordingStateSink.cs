using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Replication;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A state sink that holds a scripted number of keys, records the order of its calls, and reads the group metadata at the moments that matter.</summary>
[ThreadSafe]
internal sealed class RecordingStateSink : IReplicaStateSink
{
    private readonly List<string> _calls = [];
    private readonly string _dir;
    private readonly Lock _sync = new();
    private ulong _count;
    private uint _crc;

    internal RecordingStateSink(string dir)
    {
        _dir = dir;
    }

    /// <summary>Gets the calls in order, each by name.</summary>
    internal IReadOnlyList<string> Calls
    {
        get
        {
            lock (_sync)
                return [.. _calls];
        }
    }

    /// <summary>Gets or sets a value indicating whether <see cref="EnterAsync" /> enters the group; <see langword="false" /> models a node that leads it.</summary>
    internal bool Enters { get; set; } = true;

    /// <summary>Gets or sets the point where <see cref="EnterAsync" /> waits after it took the group, before it returns.</summary>
    internal SinkHold? EnterHold { get; set; }

    /// <summary>Gets or sets the fault the next wipe step throws, once, after it was recorded.</summary>
    internal Exception? WipeFault { get; set; }

    /// <summary>Gets or sets the point where a wipe step waits, honoring the token of the step.</summary>
    internal SinkHold? WipeHold { get; set; }

    /// <summary>Gets a value indicating whether the group was entered and not left.</summary>
    internal bool Held { get; private set; }

    /// <summary>Gets or sets the number of keys of the group the wipe still has to remove.</summary>
    internal int Keys { get; set; }

    /// <summary>Gets the applied index the transfer reseeded; <see langword="null" /> before it.</summary>
    internal ulong? ResetIndex { get; private set; }

    /// <summary>Gets the member state of both metadata slots when the journal barrier was awaited; <see langword="null" /> before it.</summary>
    internal (GroupMemberState First, GroupMemberState Second)? SlotsAtSeal { get; private set; }

    /// <summary>Gets the member state of both metadata slots at the first wipe step; <see langword="null" /> before it.</summary>
    internal (GroupMemberState First, GroupMemberState Second)? SlotsAtFirstWipe { get; private set; }

    public async ValueTask<bool> EnterAsync(string groupId, CancellationToken cancellationToken)
    {
        Record("enter");
        Held = Enters;
        _count = 0UL;
        _crc = 0U;
        if (EnterHold is { } hold)
            await hold.WaitAsync(cancellationToken).ConfigureAwait(false);

        return Enters;
    }

    public void Leave(string groupId)
    {
        Record("leave");
        Held = false;
    }

    public void ResetAppliedTo(string groupId, ulong index)
    {
        Record("reset");
        ResetIndex = index;
    }

    public async ValueTask<ReplicaStateTotals> SealAsync(string groupId, CancellationToken cancellationToken)
    {
        Record("seal");
        SlotsAtSeal = TransferFixtures.SlotStates(await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(_dir, groupId), cancellationToken).ConfigureAwait(false));
        return new ReplicaStateTotals(_count, _crc);
    }

    public async ValueTask<bool> WipeStepAsync(string groupId, int maxExamined, CancellationToken cancellationToken)
    {
        SlotsAtFirstWipe ??= TransferFixtures.SlotStates(await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(_dir, groupId), cancellationToken).ConfigureAwait(false));
        Record("wipe");
        if (WipeHold is { } hold)
            await hold.WaitAsync(cancellationToken).ConfigureAwait(false);

        if (WipeFault is { } fault)
        {
            WipeFault = null;
            throw fault;
        }

        var examined = Math.Min(maxExamined, Keys);
        Keys -= examined;
        return examined == maxExamined;
    }

    public ValueTask WriteAsync(string groupId, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken)
    {
        Record("write");
        for (var i = 0; i < entries.Length; i++)
        {
            if (entries.Span[i].Payload.IsEmpty)
                throw new InvalidDataException("The scripted sink refuses an empty payload.");

            var totals = Add(new ReplicaStateTotals(_count, _crc), entries.Span[i]);
            _count = totals.EntryCount;
            _crc = totals.Crc;
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Computes the count and the checksum this sink reports for entries written in the given order.</summary>
    /// <param name="entries">The entries.</param>
    /// <returns>The totals.</returns>
    internal static ReplicaStateTotals TotalsOf(params ReplicaStateEntry[] entries)
    {
        var totals = new ReplicaStateTotals(0UL, 0U);
        for (var i = 0; i < entries.Length; i++)
            totals = Add(totals, in entries[i]);

        return totals;
    }

    private static ReplicaStateTotals Add(ReplicaStateTotals totals, in ReplicaStateEntry entry) =>
        new(totals.EntryCount + 1UL, unchecked((totals.Crc * 31U) + entry.Payload.Span[0]));

    private void Record(string call)
    {
        lock (_sync)
            _calls.Add(call);
    }

    /// <summary>A point a sink call waits at until the test releases it, so the test can act while the call is inside the sink.</summary>
    internal sealed class SinkHold
    {
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a task that completes once a call waits at the point.</summary>
        internal Task Reached => _reached.Task;

        /// <summary>Lets the waiting call go on.</summary>
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until the test releases the point, or the token is cancelled.</summary>
        /// <param name="cancellationToken">The token of the call.</param>
        /// <returns>A task that completes when the point is released.</returns>
        internal Task WaitAsync(CancellationToken cancellationToken)
        {
            _ = _reached.TrySetResult();
            return _released.Task.WaitAsync(cancellationToken);
        }
    }
}
