using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of one live append request of a follower sender: the entry is sent alone and acknowledged by an in-process follower.</summary>
[MemoryDiagnoser]
public class ReplicaFollowerSenderBenchmarks
{
    private const int OperationsPerInvoke = 10_000;
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private static readonly byte[] Payload = [1];
    private ulong _index;
    private ReplicaFollowerSender? _sender;

    /// <summary>Disposes the sender.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_sender != null)
            await _sender.DisposeAsync().ConfigureAwait(false);

        _sender = null;
    }

    /// <summary>Creates the sender of a follower that accepts every request.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var header = new ReplicaRpcHeader("n1", Fingerprint, 1, 1, "n1", "n1");
        _sender = new ReplicaFollowerSender(new AcceptingGateway(), "n2", in header, 0, 0, TimeSpan.FromSeconds(5)) { TimeProvider = TimeProvider.System };
        _index = 0;
    }

    /// <summary>Sends one entry per request and waits for its acknowledgement.</summary>
    /// <returns>A task that completes when every entry is acknowledged.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task SendEntryAsync()
    {
        var sender = ThrowHelper.Required(_sender, "Benchmark sender was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var index = ++_index;
            var identity = new ReplicaOperationIdentity("group-a", "client", "op", Payload);
            var mutation = new PreparedReplicaMutation(identity, 1, index, new ReplicaMutationPayload(Payload, Payload, 4));
            _ = await sender.EnqueueAsync(mutation, index - 1, 1, index - 1).ConfigureAwait(false);
        }
    }

    /// <summary>Follower transport double that accepts every request.</summary>
    [Immutable]
    private sealed class AcceptingGateway : IReplicaRpcGateway
    {
        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            var last = batch.Entries.Count == 0 ? batch.PrevLogIndex : batch.Entries[^1].LogIndex;
            return Task.FromResult(new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last));
        }
    }
}
