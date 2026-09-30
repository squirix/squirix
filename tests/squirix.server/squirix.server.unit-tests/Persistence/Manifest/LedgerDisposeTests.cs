using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Order of <see cref="Ledger.Dispose" /> between manifest retention and the manifest publisher.</summary>
[Immutable]
public sealed class LedgerDisposeTests : IsolatedStorageTestBase
{
    /// <summary>Retention is stopped before the publisher drains, so a roll committed during disposal cannot schedule a cleanup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeStopsRetentionBeforeDrain(CancellationToken cancellationToken)
    {
        var ledger = new Ledger(StoreTestSupport.CreateOptions(Dir), NullLogger<Ledger>.Instance);
        await ledger.WriteAsync(new State { CurrentJournal = 1 }, cancellationToken);
        using var probe = new DrainProbe(ledger, cancellationToken);
        ledger.EnqueueRoll(2, 2, probe.HoldFirstRoll, static _ => { });
        ledger.EnqueueRoll(3, 3, probe.RecordDrainedRoll, static _ => { });
        await probe.WaitUntilHeldAsync();

        var disposing = Task.Factory.StartNew(ledger.Dispose, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        try
        {
            // The publisher is held on the first roll, so a stop that only comes after the drain never happens here and the wait times out.
            await ledger.WaitUntilAsync(static l => l.IsRetentionStopped, cancellationToken);
        }
        finally
        {
            probe.Release();
            await disposing;
        }

        _ = await Assert.That(probe.StoppedWhenDrained).IsTrue();
    }

    /// <summary>Holds the publisher on the first roll and records whether retention was stopped when the roll behind it was committed.</summary>
    private sealed class DrainProbe : IDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly TaskCompletionSource _held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Ledger _ledger;
        private readonly ManualResetEventSlim _release = new();
        private int _stoppedWhenDrained;

        internal DrainProbe(Ledger ledger, CancellationToken cancellationToken)
        {
            _ledger = ledger;
            _cancellationToken = cancellationToken;
        }

        internal bool StoppedWhenDrained => Volatile.Read(ref _stoppedWhenDrained) != 0;

        public void Dispose() => _release.Dispose();

        internal void HoldFirstRoll()
        {
            _ = _held.TrySetResult();
            _release.Wait(_cancellationToken);
        }

        internal void RecordDrainedRoll() => Volatile.Write(ref _stoppedWhenDrained, _ledger.IsRetentionStopped ? 1 : 0);

        internal void Release() => _release.Set();

        internal Task WaitUntilHeldAsync() => _held.Task.WaitAsync(_cancellationToken);
    }
}
