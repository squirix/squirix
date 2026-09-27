using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Verifies <see cref="TracingJournalCoordinatorDecorator" /> passes expected trace context to <see cref="IJournalOperationTracer" />.</summary>
[Immutable]
public sealed class TracingJournalCoordinatorDecoratorTests : IsolatedStorageTestBase
{
    /// <summary>Append put through the decorator begins a journal put trace scope.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendPutAsyncCreatesJournalPutSpan(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 600_000 };
        using var manifestStore = new Ledger(options);
        await using var core = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var tracer = new RecordingJournalOperationTracer();
        await using var journal = new TracingJournalCoordinatorDecorator(core, tracer);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutUnderGateAsync(CacheKey.Default("trace-key"), payload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        var (_, context) = await Assert.That(tracer.BeginCalls).HasSingleItem(static call => call.Kind is JournalOperationKind.Put);
        _ = await Assert.That(context.Key).IsEqualTo("trace-key");
        _ = await Assert.That(context.PayloadBytes).IsEqualTo(payload.Length);
    }

    /// <summary>Disposing the decorator leaves the journal it does not own open for its owner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeLeavesInnerJournalOpen(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 600_000 };
        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        await using var core = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        var journal = new TracingJournalCoordinatorDecorator(core, new RecordingJournalOperationTracer());

        await journal.DisposeAsync();
        await core.AppendPutUnderGateAsync(CacheKey.Default("owner-key"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
        await core.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(core.AppendedOps).IsEqualTo(1L);
    }

    /// <summary>Disposing the decorator detaches it: appends to the journal no longer reach the decorator's subscribers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeStopsForwardingAppends(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 16, FlushInterval = 600_000 };
        using var manifestStore = new Ledger(options);
        var state = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        await using var core = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true));
        var journal = new TracingJournalCoordinatorDecorator(core, new RecordingJournalOperationTracer());
        var forwarded = 0;
        journal.OnAppended += (_, _) => Interlocked.Increment(ref forwarded);

        // The journal raises to append to its handlers in subscription order: once this later handler ran, the decorator's would have too.
        var appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        core.OnAppended += (_, _) => appended.TrySetResult();

        await journal.DisposeAsync();
        await core.AppendPutUnderGateAsync(CacheKey.Default("detached-key"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
        await appended.Task.WaitAsync(cancellationToken);

        _ = await Assert.That(Volatile.Read(ref forwarded)).IsEqualTo(0);
    }

    /// <summary>Ensures traced journal puts reflect strict fsync and group-commit settings from persistence options.</summary>
    /// <param name="groupCommitMaxWaitMilliseconds">Group-commit wait window; zero disables group commit.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(5)]
    [Arguments(0)]
    public async Task PutAsyncContextReflectsDurability(int groupCommitMaxWaitMilliseconds, CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(groupCommitMaxWaitMilliseconds),
            JournalMaxSegmentMb = 16,
            FlushInterval = 600_000,
        };
        using var manifestStore = new Ledger(options);
        await using var core = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var tracer = new RecordingJournalOperationTracer();
        await using var journal = new TracingJournalCoordinatorDecorator(core, tracer);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutUnderGateAsync(CacheKey.Default("trace-key"), payload, cancellationToken);
        if (groupCommitMaxWaitMilliseconds > 0)
            await journal.AwaitDurabilityCommitAsync(cancellationToken);

        var (_, context) = await Assert.That(tracer.BeginCalls).HasSingleItem(static call => call.Kind is JournalOperationKind.Put);
        _ = await Assert.That(context.GroupCommitEnabled).IsEqualTo(groupCommitMaxWaitMilliseconds > 0);
    }

    /// <summary>Captures <see cref="IJournalOperationTracer.Begin" /> calls for decorator unit tests.</summary>
    [Immutable]
    private sealed class RecordingJournalOperationTracer : IJournalOperationTracer
    {
        private static readonly IJournalOperationTraceScope SharedScope = CreateNullScope();

        internal List<(JournalOperationKind Kind, JournalOperationTraceContext Context)> BeginCalls { get; } = [];

        IJournalOperationTraceScope? IJournalOperationTracer.Begin(JournalOperationKind kind, in JournalOperationTraceContext? context)
        {
            if (context == null)
                return null;
            BeginCalls.Add((kind, context));
            return SharedScope;
        }

        private static IJournalOperationTraceScope CreateNullScope()
        {
            var expectations = new IJournalOperationTraceScopeCreateExpectations();
            _ = expectations.Setups.Dispose();
            return expectations.Instance();
        }
    }
}
