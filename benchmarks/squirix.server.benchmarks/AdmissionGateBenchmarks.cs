using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of admitting one request through the node gate with default options and no contention, then releasing its lease.</summary>
[MemoryDiagnoser]
public class AdmissionGateBenchmarks
{
    private const string Transport = "grpc";
    private const string Operation = "get";
    private const string ClientId = "grpc:client";
    private const int OperationsPerInvoke = 10_000;
    private AdmissionGate? _gate;
    private Meter? _meter;

    /// <summary>Admits and releases one request per operation.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AcquireAndDisposeAsync()
    {
        var gate = ThrowHelper.Required(_gate, "Benchmark gate was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var (_, lease) = await gate.AcquireAsync(Transport, Operation, ClientId, CancellationToken.None).ConfigureAwait(false);
            lease.Dispose();
        }
    }

    /// <summary>Releases the gate and its meter.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _gate?.Dispose();
        _meter?.Dispose();
    }

    /// <summary>Creates the gate with default admission options.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _meter = new Meter("Squirix.Benchmarks.Admission");
        _gate = new AdmissionGate(new AdmissionOptions(), new BackpressureMetrics(_meter), TimeProvider.System);
    }
}
