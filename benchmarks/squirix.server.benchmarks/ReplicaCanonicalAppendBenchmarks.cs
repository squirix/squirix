using System;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of the append request per run of records: the leader builds the wire request from prepared mutations, the follower turns the request into the entries it appends.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class ReplicaCanonicalAppendBenchmarks
{
    private const int FingerprintSize = 16;
    private const int MutationSize = 256;
    private static readonly byte[] TopologyFingerprint = [9, 8, 7];

    private AppendReplicaEntriesRequest _request = new();
    private ReplicaRpcHeader _header;
    private ReplicationEnvelopeHeader _envelopeHeader = new();
    private PreparedReplicaMutation[] _mutations = [];

    /// <summary>Gets or sets the number of records in the run.</summary>
    [Params(1, 16)]
    public int Count { get; set; }

    /// <summary>Builds the prepared mutations and the request a follower receives for them.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _header = new ReplicaRpcHeader("n1", TopologyFingerprint, 1, 1, "n1", "n1");
        _envelopeHeader = new ReplicationEnvelopeHeader { SchemaVersion = EnvelopeSchema.Version, GroupId = "n1", Term = 1, LeaderNodeId = "n1", SenderNodeId = "n1" };
        _mutations = new PreparedReplicaMutation[Count];
        for (var i = 0; i < _mutations.Length; i++)
        {
            var index = ulong.CreateChecked(i) + 1;
            var record = new ReplicaLogRecord(
                index,
                1,
                $"5d0c2f1e7a3b4c9d8e6f1a2b3c4d5e{i:x2}",
                "user",
                new byte[FingerprintSize],
                "mutation",
                "orders",
                new byte[24],
                "Set",
                new byte[MutationSize],
                new byte[16],
                638000000000000000,
                638000000000000000,
                638000000000000000);
            var identity = new ReplicaOperationIdentity("n1", "user", record.OperationId, new byte[FingerprintSize]);
            _mutations[i] = new PreparedReplicaMutation(identity, 1, index, new ReplicaMutationPayload(ReplicaLogCodec.Encode(in record), new byte[16], 0));
        }

        _request = BuildRequest();
    }

    /// <summary>The leader checks each prepared mutation, queues it as a log entry, and builds the wire request of the run.</summary>
    /// <returns>The wire request.</returns>
    [Benchmark]
    public object LeaderBuildsRequest() => BuildRequest();

    /// <summary>The follower maps the wire request to the entries it appends, after checking each of them.</summary>
    /// <returns>The entries to append.</returns>
    /// <exception cref="InvalidOperationException">Thrown when an entry is not a canonical record at its position.</exception>
    [Benchmark]
    public object FollowerReadsRequest()
    {
        var batch = SquirixReplicationServiceAdapter.BuildFollowerBatch(_request, _envelopeHeader);
        var entries = batch.Entries as FollowerLogEntry[] ?? [.. batch.Entries];
        for (var i = 0; i < entries.Length; i++)
        {
            if (!ReplicaLogCodec.IsWellFormed(entries[i].Payload.Span, entries[i].LogIndex, entries[i].Term))
                throw new InvalidOperationException("Entry is not a canonical record at its position.");
        }

        return entries;
    }

    /// <summary>Checks and queues each prepared mutation as a log entry and maps the run to its wire request.</summary>
    /// <returns>The wire request.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a prepared payload is not a canonical record at its position.</exception>
    private AppendReplicaEntriesRequest BuildRequest()
    {
        var entries = new FollowerLogEntry[_mutations.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            var mutation = _mutations[i];
            if (!ReplicaLogCodec.IsWellFormed(mutation.CanonicalPayload.Span, mutation.LogIndex, mutation.Term))
                throw new InvalidOperationException("Prepared mutation carries an undecodable canonical payload.");

            entries[i] = new FollowerLogEntry(mutation.LogIndex, mutation.Term, mutation.CanonicalPayload);
        }

        var batch = new FollowerBatch(entries, "n1", 1, 0, 0, 0);
        return ReplicaRpcGateway.MapRequest(in _header, in batch);
    }
}
