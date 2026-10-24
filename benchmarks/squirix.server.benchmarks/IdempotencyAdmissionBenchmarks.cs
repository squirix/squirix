using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Cost of admitting one new mutation into <see cref="RpcMutationIdempotencyStore" /> when the store is full of outcomes older than the minimum
/// retention: every admission evicts one outcome, reserves the id, and records its outcome.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(ContentionBenchmarkConfig))]
public class IdempotencyAdmissionBenchmarks
{
    private const string Fingerprint = "fingerprint";
    private const int OperationsPerInvoke = 4096;
    private static readonly byte[] ResponseBytes = new Int32Value { Value = 42 }.ToByteArray();
    private readonly Meter _meter = new("idempotency-admission-bench");
    private string[] _ids = [];
    private RpcMutationIdempotencyStore? _store;

    /// <summary>Releases the meter.</summary>
    [GlobalCleanup]
    public void Cleanup() => _meter.Dispose();

    /// <summary>Creates a store at its default capacity, full of outcomes already past the minimum retention.</summary>
    [IterationSetup]
    public void IterationSetup()
    {
        var options = new IdempotencyOptions();
        var store = new RpcMutationIdempotencyStore(options, "bench-node", new IdempotencyMetrics(_meter), TimeProvider.System);
        var createdUtc = DateTime.UtcNow - options.MinRetention - TimeSpan.FromMinutes(1);
        var restored = new List<PersistedIdempotencyRecord?>(options.MaxInFlightRecords);
        for (var i = 0; i < options.MaxInFlightRecords; i++)
        {
            var id = string.Create(CultureInfo.InvariantCulture, $"full-{i}");
            restored.Add(new PersistedIdempotencyRecord(id, Fingerprint, ResponseBytes, createdUtc));
        }

        store.RestoreSnapshotRecords(restored);
        _store = store;
        _ids = new string[OperationsPerInvoke];
        for (var i = 0; i < _ids.Length; i++)
            _ids[i] = string.Create(CultureInfo.InvariantCulture, $"new-{i}");
    }

    /// <summary>Admits new operations on the full store: reserve, record the outcome, complete the execution.</summary>
    /// <returns>Operations admitted, as a sink.</returns>
    /// <exception cref="InvalidOperationException">A reservation was refused.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public int AdmitOnFullTable()
    {
        var store = ThrowHelper.Required(_store, "Benchmark store was not initialized.");
        var admitted = 0;
        foreach (var operationId in _ids)
        {
            var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (store.ReserveIntent(operationId, Fingerprint, execution, out _) != IdempotencyReserveResult.Acquired)
                throw new InvalidOperationException("Reservation was not acquired.");

            store.RecordSuccess(operationId, Fingerprint, ResponseBytes, execution);
            store.CompleteExecution(operationId, execution);
            admitted++;
        }

        return admitted;
    }
}
