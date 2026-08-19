using System;
using System.Globalization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates segment-roll manifest costs: data-file fsync, pointer fsync, and full publish.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 2)]
public class ManifestPublishBreakdownBenchmarks
{
    private int _nextFileIndex = 10_000;
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
        ResetFileIndex();
        _nextJournal = 2;
        _nextSequence = 2;
    }

    /// <summary>Full production roll publish path via manifest store roll blocking API.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark(Baseline = true)]
    public Task PublishRollBlockingAsync()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
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

    /// <summary>Creates a new <c>.bmqx</c> file and fsyncs it using a fixed pre-encoded roll payload.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollDataFileOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var encodedLength = session.EncodeRoll(1, 1);
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
        {
            var path = session.BuildManifestFilePath(TakeNextFileIndex());
            session.WriteDataFile(path, encodedLength);
        }
    }

    /// <summary>Roll encode plus numbered manifest file write (no pointer update).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollEncodeAndDataFile()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
        {
            var encodedLength = session.EncodeRoll(_nextJournal++, _nextSequence++);
            var path = session.BuildManifestFilePath(TakeNextFileIndex());
            session.WriteDataFile(path, encodedLength);
        }
    }

    /// <summary>Overwrites <c>man-current</c> and fsyncs the pointer (no numbered manifest file).</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark session was not initialized.</exception>
    [Benchmark]
    public void RollPointerOnly()
    {
        var session = _session ?? throw new InvalidOperationException("Benchmark session was not initialized.");
        var operations = _operationsPerInvoke;
        for (var i = 0; i < operations; i++)
            session.WritePointer(TakeNextFileIndex());
    }

    private void ResetFileIndex() => _nextFileIndex = 10_000;

    private int TakeNextFileIndex() => _nextFileIndex++;

    /// <summary>Hosts a warmed manifest store for roll-path breakdown benchmarks.</summary>
    [Immutable]
    private sealed class Session : IDisposable
    {
        private const int EncodeBufferSize = 512;

        private readonly TempDirectory _dataDir;
        private readonly byte[] _encodeBuffer;

        private Session(TempDirectory dataDir, Ledger store, IManifestPointerWriter pointerWriter, SessionWarmup warmup)
        {
            _dataDir = dataDir;
            _encodeBuffer = warmup.EncodeBuffer;
            Ledger = store;
            Format = warmup.Format;
            Snapshot = warmup.Snapshot;
            SnapshotPathUtf8 = warmup.SnapshotPathUtf8;
            ManifestFileNamePrefix = warmup.ManifestFileNamePrefix;
            PointerWriter = pointerWriter;
        }

        internal Ledger Ledger { get; }

        private int Format { get; }

        private string ManifestFileNamePrefix { get; }

        private IManifestPointerWriter PointerWriter { get; }

        private SnapshotRef? Snapshot { get; }

        private byte[] SnapshotPathUtf8 { get; }

        public void Dispose()
        {
            PointerWriter.Dispose();
            Ledger.Dispose();
            _dataDir.Dispose();
        }

        /// <summary>Creates a warmed manifest session with primed in-memory cache.</summary>
        /// <returns>A session ready for breakdown benchmarks.</returns>
        internal static async Task<Session> CreateAsync()
        {
            var dataDir = new TempDirectory("manifest-breakdown");
            var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
            var options = new PersistenceOptions
            {
                DataDir = dataDir.Path,
                ManifestRetentionCount = retention,
                SnapshotRetentionCount = retention,
            };
            var store = new Ledger(options);
            var warmup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            store.EnqueueRoll(1, 1, warmup.SetResult, warmup.SetException);
            await warmup.Task.ConfigureAwait(false);

            var encodeBuffer = new byte[EncodeBufferSize];
            var manifestFileNamePrefix = PathEx.Combine(dataDir.Path, FilePrefixes.Manifest);
            var currentPath = PathEx.Combine(dataDir.Path, $"{FilePrefixes.Manifest}current");
            var pointerWriter = new PersistentPointerWriter(currentPath);

            return new Session(
                dataDir,
                store,
                pointerWriter,
                new SessionWarmup
                {
                    Format = 1,
                    Snapshot = null,
                    SnapshotPathUtf8 = [],
                    EncodeBuffer = encodeBuffer,
                    ManifestFileNamePrefix = manifestFileNamePrefix,
                });
        }

        /// <summary>Builds a numbered manifest file path under the session data directory.</summary>
        /// <param name="index">One-based manifest file index.</param>
        /// <returns>Absolute path to a <c>.bmqx</c> file.</returns>
        internal string BuildManifestFilePath(int index) => string.Create(
            ManifestFileNamePrefix.Length + 6 + FileExtensions.Manifest.Length,
            (Prefix: ManifestFileNamePrefix, Index: index),
            static (span, state) =>
            {
                state.Prefix.CopyTo(span);
                var suffix = span[state.Prefix.Length..];
                if (!state.Index.TryFormat(suffix, out var charsWritten, "D6", CultureInfo.InvariantCulture))
                    throw new InvalidOperationException("Manifest index did not fit fixed-width field.");

                FileExtensions.Manifest.CopyTo(suffix[charsWritten..]);
            });

        /// <summary>Encodes a segment-roll manifest into the session encode buffer.</summary>
        /// <param name="currentJournal">Updated current journal segment index.</param>
        /// <param name="nextSequence">Updated next journal sequence.</param>
        /// <returns>Encoded byte length.</returns>
        internal int EncodeRoll(int currentJournal, ulong nextSequence) =>
            FileCodec.WriteRollEncoded(Format, currentJournal, nextSequence, Snapshot, SnapshotPathUtf8, _encodeBuffer);

        /// <summary>Writes a pre-encoded manifest file and flushes it to disk.</summary>
        /// <param name="targetPath">Path to a new <c>.bmqx</c> file.</param>
        /// <param name="encodedLength">Number of valid bytes in the session encode buffer.</param>
        internal void WriteDataFile(string targetPath, int encodedLength) => FileDurability.WriteManifestDataFileBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength));

        /// <summary>Writes the SQMC current pointer and flushes it to disk.</summary>
        /// <param name="manifestIndex">Manifest index for the pointer payload.</param>
        internal void WritePointer(int manifestIndex)
        {
            Span<byte> pointerBuffer = stackalloc byte[Pointer.Size];
            Pointer.Write(pointerBuffer, manifestIndex);
            FileDurability.WriteCurrentPointerBlocking(PointerWriter, pointerBuffer);
        }

        /// <summary>Non-owned warmup values for <see cref="Session" /> construction (avoids an 8+ parameter ctor).</summary>
        [Immutable]
        private sealed class SessionWarmup
        {
            internal required byte[] EncodeBuffer { get; init; }

            internal required int Format { get; init; }

            internal required string ManifestFileNamePrefix { get; init; }

            internal required SnapshotRef? Snapshot { get; init; }

            internal required byte[] SnapshotPathUtf8 { get; init; }
        }
    }
}
