using System;
using System.IO;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates segment-roll manifest costs: the two-slot write and the full publish.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class ManifestPublishBreakdownBenchmarks
{
    private int _nextJournal = 2;
    private ulong _nextSequence = 2;
    private int _operationsPerInvoke;
    private Session? _session;

    /// <summary>Disposes the breakdown session and temp data directory.</summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _session?.Dispose();
        _session = null;
    }

    /// <summary>Creates a warmed manifest session for the current parameter set.</summary>
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _operationsPerInvoke = ManifestBenchmarkSupport.ResolvePublishOperationsPerInvoke();
        _session = await Session.CreateAsync().ConfigureAwait(false);
        _nextJournal = 2;
        _nextSequence = 2;
    }

    /// <summary>Full production roll publish path via manifest store roll blocking API.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark(Baseline = true)]
    public Task PublishRollBlockingAsync()
    {
        var session = ThrowHelper.Required(_session, "Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        for (var i = 0; i < operations; i++)
            session.Ledger.EnqueueRoll(_nextJournal++, _nextSequence++, i == operations - 1 ? OnSuccess : static () => { }, OnFailure);

        return completion.Task;

        void OnFailure(Exception ex)
        {
            completion.TrySetException(ex);
        }

        void OnSuccess()
        {
            completion.TrySetResult();
        }
    }

    /// <summary>Writes both <c language="csharp">man-current</c> slots and fsyncs them (encode and slot writes only).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollSlotsOnly()
    {
        var session = ThrowHelper.Required(_session, "Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
            session.WriteSlots(_nextJournal++, _nextSequence++);
    }

    /// <summary>Hosts a warmed manifest store for roll-path breakdown benchmarks.</summary>
    [Immutable]
    private sealed class Session : IDisposable
    {
        private readonly TempDirectory _dir;

        private Session(TempDirectory dir, Ledger store, SlotWriter slotWriter)
        {
            _dir = dir;
            Ledger = store;
            SlotWriter = slotWriter;
        }

        internal Ledger Ledger { get; }

        private SlotWriter SlotWriter { get; }

        public void Dispose()
        {
            SlotWriter.Dispose();
            Ledger.Dispose();
            _dir.Dispose();
        }

        /// <summary>Creates a warmed manifest session with primed in-memory cache.</summary>
        /// <returns>A session ready for breakdown benchmarks.</returns>
        internal static async Task<Session> CreateAsync()
        {
            var dir = new TempDirectory("manifest-breakdown");
            var options = new PersistenceOptions
            {
                DataDir = dir,
            };
            var store = new Ledger(options, NullLogger<Ledger>.Instance);
            var warmup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            store.EnqueueRoll(1, 1, warmup.SetResult, warmup.SetException);
            await warmup.Task.ConfigureAwait(false);

            // The ledger keeps its own writer on dir/man-current open, so the slots-only benchmark writes a separate file.
            var slotsDir = PathEx.Combine(dir, "slots-only");
            _ = Directory.CreateDirectory(slotsDir);
            var currentPath = PathEx.Combine(slotsDir, $"{FilePrefixes.Manifest}current");

            return new Session(dir, store, new SlotWriter(currentPath));
        }

        /// <summary>Writes a roll manifest into both slots and flushes it to disk.</summary>
        /// <param name="currentJournal">Updated current journal segment index.</param>
        /// <param name="nextSequence">Updated next journal sequence.</param>
        internal void WriteSlots(int currentJournal, ulong nextSequence)
        {
            _ = SlotWriter.Publish(new State { CurrentJournal = currentJournal, NextSequence = nextSequence });
            SlotWriter.Mirror();
        }
    }
}
