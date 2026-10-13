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

    /// <summary>Gets or sets the step the gateway refuses.</summary>
    internal string? RefuseAt { get; set; }

    /// <summary>Gets or sets the refusal code of <see cref="RefuseAt" />.</summary>
    internal string RefusalCode { get; set; } = RefusalCodes.TransferBusy;

    /// <summary>Gets or sets the term every reply carries.</summary>
    internal ulong ReplyTerm { get; set; } = 1UL;

    /// <summary>Gets or sets the number of wipe replies that ask for another wipe step.</summary>
    internal int WipeSteps
    {
        get => WipesLeft();
        set => Volatile.Write(ref _wipesLeft, value);
    }

    /// <summary>Gets or sets a hook that runs when the end message is accepted.</summary>
    internal Action? OnEnd { get; set; }

    /// <summary>Gets or sets a hook that sees the entries of every entries message while the call is in flight.</summary>
    internal Action<ReadOnlyMemory<ReplicaStateEntry>>? OnEntries { get; set; }

    /// <summary>Gets the entries every entries message carried, copied when it arrived.</summary>
    internal List<ReplicaStateEntry[]> Sent { get; } = [];

    /// <inheritdoc />
    public Task<ReplicaTransferReply> BeginAsync(string nodeId, ReplicaRpcHeader header, string transferId, ReplicaTransferBaseline baseline, CancellationToken cancellationToken)
    {
        _transferIds.Enqueue(transferId);
        return StepAsync("begin", cancellationToken);
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

        return StepAsync(name, cancellationToken);
    }

    /// <inheritdoc />
    public Task<ReplicaTransferReply> AbortAsync(string nodeId, ReplicaRpcHeader header, string transferId, CancellationToken cancellationToken)
    {
        Calls.Enqueue("abort");
        _ = Interlocked.Increment(ref _aborts);
        return Task.FromResult(ReplicaTransferReply.Accepted(ReplyTerm));
    }

    /// <summary>Lets the held step, and every later one, go on.</summary>
    internal void Release() => _ = _release.TrySetResult();

    private int WipesLeft() => Volatile.Read(ref _wipesLeft);

    private async Task<ReplicaTransferReply> StepAsync(string name, CancellationToken cancellationToken)
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

        if (string.Equals(RefuseAt, name, StringComparison.Ordinal))
            return ReplicaTransferReply.Refused(ReplyTerm, RefusalCode);

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
