using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Durable mutation group-commit throughput via <see cref="DurableMutationExecutor" />. With <see cref="IdempotentScope" /> each mutation runs the way
/// a hosted RPC does: through <see cref="RpcMutationIdempotencyCoordinator" />. The handler registers its outcome projection and the mutation predicts
/// its result, so the outcome frame is appended right behind the mutation frame and one flush covers both.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class DurableMutationGroupCommitBenchmarks
{
    private const int DefaultOperationsPerWriter = 2_000;
    private const string Fingerprint = "bench-fingerprint";
    private readonly Meter _meter = new("durable-mutation-gc-bench");
    private RpcMutationIdempotencyCoordinator? _coordinator;
    private DurableMutationExecutor? _executor;
    private JournalBenchmarkHost? _host;
    private int _nextWriterId;
    private byte[] _putPayload = [];

    /// <summary>Gets or sets the journal group-commit maximum wait in milliseconds; 0 turns group commit off.</summary>
    [Params(0, 1, 5)]
    public int GroupCommitMaxWaitMs { get; set; }

    /// <summary>Gets or sets a value indicating whether all concurrent writers mutate one key instead of distinct keys.</summary>
    [Params(false, true)]
    public bool HotKey { get; set; }

    /// <summary>Gets or sets a value indicating whether each mutation runs through the RPC idempotency coordinator (outcome frame and durability wait).</summary>
    [Params(false, true)]
    public bool IdempotentScope { get; set; }

    /// <summary>Gets or sets the number of concurrent writers.</summary>
    [Params(8, 64, 256)]
    public int Writers { get; set; }

    /// <summary>Gets or sets the PUT payload size in bytes.</summary>
    [Params(256, 4096)]
    public int PutPayloadBytes { get; set; }

    /// <summary>Disposes the journal coordinator and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _executor = null;
        _coordinator = null;
        _meter.Dispose();
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Runs durable PUT mutations with per-key group commit (production-like path).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark executor was not initialized.</exception>
    [Benchmark]
    public Task ExecutePutMutationAsync()
    {
        var host = ThrowHelper.Required(_host, "Benchmark host was not initialized.");
        var executor = ThrowHelper.Required(_executor, "Benchmark executor was not initialized.");
        var coordinator = IdempotentScope ? ThrowHelper.Required(_coordinator, "Benchmark idempotency coordinator was not initialized.") : null;
        var hotKey = HotKey;
        var payload = _putPayload;
        var operationsPerWriter = GetOperationsPerWriter();
        var parallelWriters = Writers;
        return Parallel.ForEachAsync(
            new int[parallelWriters],
            new ParallelOptions { MaxDegreeOfParallelism = parallelWriters },
            (_, cancellationToken) =>
            {
                var writerId = Interlocked.Increment(ref _nextWriterId);
                var key = new CacheKey("bench", hotKey ? "hot" : $"m{NodeInvariantIndexStrings.Format(writerId)}");
                return RunWriterAsync(coordinator, (executor, host.Coordinator, key, payload), writerId, operationsPerWriter, cancellationToken);
            });
    }

    /// <summary>Creates the journal coordinator, executor, and payload for the current parameter set.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(GroupCommitMaxWaitMs),
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("durable-mutation-gc-bench", options, CancellationToken.None).ConfigureAwait(false);
        _executor = new DurableMutationExecutor(_host.Coordinator, NullLogger<DurableMutationExecutor>.Instance);
        var idempotencyOptions = new IdempotencyOptions { MaxInFlightRecords = 4_000_000 };
        _coordinator = new RpcMutationIdempotencyCoordinator(
            new RpcMutationIdempotencyStore(idempotencyOptions, "bench", new IdempotencyMetrics(_meter)),
            _host.Coordinator,
            NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        _putPayload = new byte[PutPayloadBytes];
        Array.Fill(_putPayload, Convert.ToByte('m'));
        _nextWriterId = 0;
    }

    private static async ValueTask RunWriterAsync(
        RpcMutationIdempotencyCoordinator? coordinator,
        (DurableMutationExecutor Executor, IJournalCoordinator Journal, CacheKey Key, byte[] Payload) pipelineState,
        int writerId,
        int operationsPerWriter,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < operationsPerWriter; i++)
        {
            if (coordinator == null)
            {
                _ = await PutAsync(pipelineState, cancellationToken).ConfigureAwait(false);
                continue;
            }

            _ = await coordinator.ExecuteAsync(
                FormatOperationId(writerId, i),
                Fingerprint,
                pipelineState,
                static async (s, ct) =>
                {
                    RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<int>(static predicted => new Int32Value { Value = predicted });
                    _ = await PutAsync(s, ct).ConfigureAwait(false);
                    return new Int32Value { Value = 1 };
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static string FormatOperationId(int writerId, int sequence) =>
        string.Create(
            32,
            (writerId, sequence),
            static (span, s) =>
            {
                _ = s.writerId.TryFormat(span[..16], out _, "x16", CultureInfo.InvariantCulture);
                _ = s.sequence.TryFormat(span[16..], out _, "x16", CultureInfo.InvariantCulture);
            });

    private static ValueTask<int> PutAsync((DurableMutationExecutor Executor, IJournalCoordinator Journal, CacheKey Key, byte[] Payload) s, CancellationToken cancellationToken) =>
        s.Executor.ExecuteAsync(
            s.Key,
            static (_, _) => ValueTask.FromResult(DurableMutationCondition<int>.Apply(1)),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, ReadOnlyMemory<byte> Payload), int>(
                (s.Journal, s.Key, s.Payload),
                static (p, ownership, ct) => p.Journal.AppendPutAsync(ownership, p.Key, p.Payload, ct),
                static (_, _) => new ValueTask<int>(1),
                static (_, predicted) => RpcMutationIdempotencyExecutionAmbient.AppendPredictedOutcomeAsync(predicted),
                static _ => RpcMutationIdempotencyExecutionAmbient.PromoteOutcomeAfterApply()),
            cancellationToken);

    private static int GetOperationsPerWriter() => JournalBenchmarkSupport.ResolveGroupCommitOperationsPerWriter(DefaultOperationsPerWriter);
}
