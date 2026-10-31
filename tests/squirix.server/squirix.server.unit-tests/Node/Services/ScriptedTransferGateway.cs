using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Transfer transport double: records every message of a state transfer, answers each step as scripted, and can hold a step until the test releases it.</summary>
internal sealed class ScriptedTransferGateway : IReplicaTransferGateway
{
    private readonly ConcurrentDictionary<string, int> _busy = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, int> _begins = new(StringComparer.Ordinal);
    private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentQueue<string> _transferIds = new();
    private int _aborts;
    private int _wipesLeft;

    /// <summary>Gets the step names of every call, in order: begin, snapshot, wipe, entries, end, abort.</summary>
    internal ConcurrentQueue<string> Calls { get; } = new();

    /// <summary>Gets the number of entries of every entries message, in order.</summary>
    internal ConcurrentQueue<string> Batches { get; } = new();

    /// <summary>Gets the sequence numbers of every message after the begin, in order.</summary>
    internal ConcurrentQueue<string> Sequences { get; } = new();

    /// <summary>Gets the number of aborts received.</summary>
    internal int Aborts => Volatile.Read(ref _aborts);

    /// <summary>Gets the count and the checksum the end message carried.</summary>
    internal (ulong Count, uint Crc)? End { get; private set; }

    /// <summary>Gets the number of different transfer identifiers the calls carried.</summary>
    internal int TransferIdCount
    {
        get
        {
            var distinct = new HashSet<string>(StringComparer.Ordinal);
            foreach (var id in _transferIds)
                _ = distinct.Add(id);

            return distinct.Count;
        }
    }

    /// <summary>Gets a task that completes once a held step is waiting.</summary>
    internal Task Held => _held.Task;

    /// <summary>Gets or sets the step the gateway holds until <see cref="Release" /> runs or the call is canceled.</summary>
    internal string? HoldAt { get; set; }

    /// <summary>Gets or sets a value indicating whether a held step that is canceled fails in transport, as a call canceled mid-flight does on a real channel.</summary>
    internal bool CancelFailsInTransport { get; set; }

    /// <summary>Gets or sets the step the gateway fails in transport.</summary>
    internal string? FailAt { get; set; }

    /// <summary>Gets or sets the only follower the busy answers apply to; every follower when not set.</summary>
    internal string? BusyNode { get; set; }

    /// <summary>Gets or sets the step the gateway refuses.</summary>
    internal string? RefuseAt { get; set; }

    /// <summary>Gets or sets the only follower that refuses a step; every follower when not set.</summary>
    internal string? RefusalNode { get; set; }

    /// <summary>Gets or sets the refusal code of <see cref="RefuseAt" />.</summary>
    internal string RefusalCode { get; set; } = RefusalCodes.TransferBusy;

    /// <summary>Gets or sets the term every reply carries.</summary>
    internal ulong ReplyTerm { get; set; } = 1UL;

    /// <summary>Gets or sets a hook that runs when the end message is accepted.</summary>
    internal Action? OnEnd { get; set; }

    /// <summary>Gets or sets a hook that sees the entries of every entries message while the call is in flight.</summary>
    internal Action<ReadOnlyMemory<ReplicaStateEntry>>? OnEntries { get; set; }

    /// <summary>Gets the entries every entries message carried, copied when it arrived.</summary>
    internal List<ReplicaStateEntry[]> Sent { get; } = [];

    /// <summary>Gets the baseline of the last begin sent.</summary>
    internal ReplicaTransferBaseline? Baseline { get; private set; }

    /// <inheritdoc />
    public Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken)
    {
        _ = _begins.AddOrUpdate(nodeId, 1, static (_, count) => count + 1);
        _transferIds.Enqueue(transferId);
        Baseline = baseline;
        return StepAsync("begin", nodeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<ReplicaTransferReply> SendAsync(string nodeId, ReplicaRpcHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken)
    {
        _transferIds.Enqueue(message.TransferId);
        Sequences.Enqueue(message.Sequence.ToString(CultureInfo.InvariantCulture));
        var name = message.Step switch
        {
            ReplicaTransferStep.Snapshot => "snapshot",
            ReplicaTransferStep.Wipe => "wipe",
            ReplicaTransferStep.Entries => "entries",
            ReplicaTransferStep.End => "end",
            _ => throw new ArgumentOutOfRangeException(nameof(message), message.Step, "Unsupported transfer step."),
        };
        if (message.Step == ReplicaTransferStep.Entries)
        {
            Batches.Enqueue(message.Entries.Length.ToString(CultureInfo.InvariantCulture));
            lock (Sent)
                Sent.Add(message.Entries.ToArray());

            OnEntries?.Invoke(message.Entries);
        }

        if (message.Step == ReplicaTransferStep.End)
            End = (message.EntryCount, message.EntriesCrc);

        return StepAsync(name, nodeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<ReplicaTransferReply> AbortAsync(string nodeId, ReplicaRpcHeader header, string transferId, CancellationToken cancellationToken)
    {
        Calls.Enqueue("abort");
        _ = Interlocked.Increment(ref _aborts);
        return Task.FromResult(ReplicaTransferReply.Accepted(ReplyTerm));
    }

    /// <summary>Makes the next calls of a step answer journal-busy.</summary>
    /// <param name="step">The step name: begin, snapshot, wipe, entries or end.</param>
    /// <param name="count">The number of calls to answer as busy before the step is accepted again.</param>
    internal void BusyFor(string step, int count) => _busy[step] = count;

    /// <summary>Counts the transfers begun for one follower.</summary>
    /// <param name="nodeId">The target follower.</param>
    /// <returns>The begin count.</returns>
    internal int BeginsFor(string nodeId) => _begins.TryGetValue(nodeId, out var count) ? count : 0;

    /// <summary>Sets the number of wipe replies that ask for another wipe step.</summary>
    /// <param name="steps">The number of wipe steps.</param>
    internal void SetWipeSteps(int steps) => Volatile.Write(ref _wipesLeft, steps);

    /// <summary>Lets the held step, and every later one, go on.</summary>
    internal void Release() => _ = _release.TrySetResult();

    private async Task<ReplicaTransferReply> StepAsync(string name, string nodeId, CancellationToken cancellationToken)
    {
        Calls.Enqueue(name);
        if (string.Equals(FailAt, name, StringComparison.Ordinal))
            throw new TimeoutException($"Injected transport failure at {name}.");

        if (string.Equals(HoldAt, name, StringComparison.Ordinal))
        {
            _ = _held.TrySetResult();
            try
            {
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException canceled) when (CancelFailsInTransport && cancellationToken.IsCancellationRequested)
            {
                throw new IOException($"Injected transport failure of a canceled call at {name}.", canceled);
            }
        }

        if ((RefusalNode == null || string.Equals(RefusalNode, nodeId, StringComparison.Ordinal)) && string.Equals(RefuseAt, name, StringComparison.Ordinal))
            return ReplicaTransferReply.Refused(ReplyTerm, RefusalCode);

        if ((BusyNode == null || string.Equals(BusyNode, nodeId, StringComparison.Ordinal)) && _busy.TryGetValue(name, out var busyLeft) && busyLeft > 0)
        {
            _busy[name] = busyLeft - 1;
            return ReplicaTransferReply.Refused(ReplyTerm, RefusalCodes.JournalBusy);
        }

        if (string.Equals(name, "wipe", StringComparison.Ordinal) && Volatile.Read(ref _wipesLeft) > 0)
        {
            _ = Interlocked.Decrement(ref _wipesLeft);
            return ReplicaTransferReply.Accepted(ReplyTerm, true);
        }

        if (string.Equals(name, "end", StringComparison.Ordinal))
            OnEnd?.Invoke();

        return ReplicaTransferReply.Accepted(ReplyTerm);
    }
}
