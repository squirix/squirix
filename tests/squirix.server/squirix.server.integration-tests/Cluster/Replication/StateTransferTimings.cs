using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Records when the phases of a state transfer run on the follower and how much it receives, by wrapping the state sink of the node.</summary>
internal sealed class StateTransferTimings
{
    private long _bytes;
    private long _entered;
    private long _entries;
    private long _firstWrite;
    private long _lastWrite;
    private long _sealed;
    private long _wipeSteps;

    /// <summary>Gets the time the transfer entered the group.</summary>
    internal long EnteredTimestamp => Volatile.Read(ref _entered);

    /// <summary>Gets the time the follower sealed the group, which ends the transfer on the follower.</summary>
    internal long SealedTimestamp => Volatile.Read(ref _sealed);

    /// <summary>Gets the time the first batch of entries was written.</summary>
    internal long FirstWriteTimestamp => Volatile.Read(ref _firstWrite);

    /// <summary>Gets the time the last batch of entries was written.</summary>
    internal long LastWriteTimestamp => Volatile.Read(ref _lastWrite);

    /// <summary>Gets the number of entries written to the follower by transfers.</summary>
    internal long Entries => Volatile.Read(ref _entries);

    /// <summary>Gets the key and payload bytes written to the follower by transfers.</summary>
    internal long Bytes => Volatile.Read(ref _bytes);

    /// <summary>Gets the number of wipe steps the follower ran.</summary>
    internal long WipeSteps => Volatile.Read(ref _wipeSteps);

    /// <summary>Wraps the state sink of the node so the transfer is timed.</summary>
    /// <param name="services">The node service collection.</param>
    /// <exception cref="InvalidOperationException">The node registers no state sink factory.</exception>
    internal void Configure(IServiceCollection services)
    {
        var index = -1;
        for (var i = services.Count - 1; i >= 0 && index < 0; i--)
        {
            if (services[i].ServiceType == typeof(IReplicaStateSink) && services[i].ImplementationFactory != null)
                index = i;
        }

        if (index < 0)
            throw new InvalidOperationException("The node registers no state sink factory.");

        var factory = services[index].ImplementationFactory!;
        services[index] = ServiceDescriptor.Singleton<IReplicaStateSink>(
            provider => factory(provider) is IReplicaStateSink inner ? new TimedSink(inner, this) : throw new InvalidOperationException("The registered state sink is not a state sink."));
    }

    private sealed class TimedSink : IReplicaStateSink
    {
        private readonly StateTransferTimings _timings;
        private readonly IReplicaStateSink _inner;

        internal TimedSink(IReplicaStateSink inner, StateTransferTimings timings)
        {
            _inner = inner;
            _timings = timings;
        }

        public async ValueTask<bool> EnterAsync(string groupId, CancellationToken cancellationToken)
        {
            var entered = await _inner.EnterAsync(groupId, cancellationToken);
            _ = Interlocked.CompareExchange(ref _timings._entered, Stopwatch.GetTimestamp(), 0);
            return entered;
        }

        public ValueTask<bool> WipeStepAsync(string groupId, int maxExamined, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _timings._wipeSteps);
            return _inner.WipeStepAsync(groupId, maxExamined, cancellationToken);
        }

        public async ValueTask WriteAsync(string groupId, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken)
        {
            _ = Interlocked.CompareExchange(ref _timings._firstWrite, Stopwatch.GetTimestamp(), 0);
            await _inner.WriteAsync(groupId, entries, cancellationToken);
            Volatile.Write(ref _timings._lastWrite, Stopwatch.GetTimestamp());
            _ = Interlocked.Add(ref _timings._entries, entries.Length);
            _ = Interlocked.Add(ref _timings._bytes, VolumeOf(entries));
        }

        public async ValueTask<ReplicaStateTotals> SealAsync(string groupId, CancellationToken cancellationToken)
        {
            var totals = await _inner.SealAsync(groupId, cancellationToken);
            Volatile.Write(ref _timings._sealed, Stopwatch.GetTimestamp());
            return totals;
        }

        public void ResetAppliedTo(string groupId, ulong index) => _inner.ResetAppliedTo(groupId, index);

        public void Leave(string groupId) => _inner.Leave(groupId);

        private static long VolumeOf(ReadOnlyMemory<ReplicaStateEntry> entries)
        {
            long bytes = 0;
            var span = entries.Span;
            for (var i = 0; i < span.Length; i++)
                bytes += span[i].Key.Length + span[i].Payload.Length;

            return bytes;
        }
    }
}
