using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
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
    private static readonly IJournalOperationTraceScope SharedScope = CreateNullScope();

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
            new AsyncManualResetEvent(true),
            NullLogger.Instance);
        var beginCalls = new List<(JournalOperationKind Kind, JournalOperationTraceContext Context)>();
        await using var journal = new TracingJournalCoordinatorDecorator(core, CreateRecordingTracer(beginCalls));

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutUnderGateAsync(CacheKey.Default("trace-key"), payload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        var (_, context) = await Assert.That(beginCalls).HasSingleItem(static call => call.Kind is JournalOperationKind.Put);
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
        await using var core = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true), NullLogger.Instance);
        var journal = new TracingJournalCoordinatorDecorator(core, new IJournalOperationTracerCreateExpectations().Instance());

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
        await using var core = JournalCoordinatorFactory.Create(options, state, manifestStore, new AsyncManualResetEvent(true), NullLogger.Instance);
        var journal = new TracingJournalCoordinatorDecorator(core, new IJournalOperationTracerCreateExpectations().Instance());
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
            new AsyncManualResetEvent(true),
            NullLogger.Instance);
        var beginCalls = new List<(JournalOperationKind Kind, JournalOperationTraceContext Context)>();
        await using var journal = new TracingJournalCoordinatorDecorator(core, CreateRecordingTracer(beginCalls));

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutUnderGateAsync(CacheKey.Default("trace-key"), payload, cancellationToken);
        if (groupCommitMaxWaitMilliseconds > 0)
            await journal.AwaitDurabilityCommitAsync(cancellationToken);

        var (_, context) = await Assert.That(beginCalls).HasSingleItem(static call => call.Kind is JournalOperationKind.Put);
        _ = await Assert.That(context.GroupCommitEnabled).IsEqualTo(groupCommitMaxWaitMilliseconds > 0);
    }

    /// <summary>Mocks a tracer that records <see cref="IJournalOperationTracer.Begin" /> calls that carry a trace context.</summary>
    /// <param name="beginCalls">Receives each traced operation kind with its context.</param>
    /// <returns>The mocked tracer.</returns>
    private static IJournalOperationTracer CreateRecordingTracer(List<(JournalOperationKind Kind, JournalOperationTraceContext Context)> beginCalls)
    {
        var expectations = new IJournalOperationTracerCreateExpectations();
        _ = expectations.Setups.Begin(Arg.Any<JournalOperationKind>(), Arg.Any<JournalOperationTraceContext?>())
                        .Callback((kind, context) =>
                         {
                             if (context == null)
                                 return null;
                             beginCalls.Add((kind, context));
                             return SharedScope;
                         });
        return expectations.Instance();
    }

    private static IJournalOperationTraceScope CreateNullScope()
    {
        var expectations = new IJournalOperationTraceScopeCreateExpectations();
        _ = expectations.Setups.Dispose();
        return expectations.Instance();
    }
}
