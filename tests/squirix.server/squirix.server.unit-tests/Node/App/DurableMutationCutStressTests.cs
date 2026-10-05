using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Node.App;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>Snapshot cuts keep completing while grouped writers write without pause.</summary>
[Immutable]
[Property(StressTrait.TraitName, StressTrait.TraitValue)]
public sealed class DurableMutationCutStressTests : IsolatedStorageTestBase
{
    private const int CutsRequired = 20;
    private const int WriterCount = 32;

    /// <summary>Only catches a cut that never captures: a cut slows with machine load, and a tight bound turns scheduling delay into a failure.</summary>
    private static readonly TimeSpan CutBound = TimeSpan.FromSeconds(15);

    private static readonly TimeSpan CutPeriod = TimeSpan.FromMilliseconds(100);

    /// <summary>Longest run: a loaded machine slows each cut, so the run extends up to this limit until the required cuts completed.</summary>
    private static readonly TimeSpan RunLimit = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan RunLength = TimeSpan.FromSeconds(5);

    /// <summary>
    /// With writers on distinct keys that never pause, every requested cut still captures, and at capture no admitted
    /// writer is left unapplied.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutsCompleteUnderSustainedGroupedWrites(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, TimeSpan.FromMilliseconds(2), 32, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        using var stop = new CancellationTokenSource();
        var writers = new List<Task>(WriterCount);
        for (var i = 0; i < WriterCount; i++)
            writers.Add(WriteUntilStoppedAsync(memory, executor, journal, string.Create(CultureInfo.InvariantCulture, $"key-{i}"), stop.Token));

        var completed = 0;
        var timedOut = 0;
        var capturedWithPending = 0;
        Task? stuckCut = null;
        try
        {
            var started = TimeProvider.System.GetTimestamp();
            while (TimeProvider.System.GetElapsedTime(started) < RunLength || (completed < CutsRequired && TimeProvider.System.GetElapsedTime(started) < RunLimit))
            {
                await Task.Delay(CutPeriod, TimeProvider.System, cancellationToken);
                var cut = journal.Journal.ExecuteSnapshotCutAsync(
                    journal.Journal,
                    static (j, _, _) => new ValueTask<bool>(j.InFlightApplyGate.HasPending),
                    static (_, _, pending, _) => new ValueTask<bool>(pending),
                    cancellationToken).AsTask();
                try
                {
                    if (await cut.WaitAsync(CutBound, TimeProvider.System, cancellationToken))
                        capturedWithPending++;

                    completed++;
                }
                catch (TimeoutException)
                {
                    timedOut++;
                    stuckCut = cut;
                    break;
                }
            }
        }
        finally
        {
            await stop.CancelAsync();
            await Task.WhenAll(writers);
            if (stuckCut != null)
                await stuckCut;
        }

        _ = await Assert.That(timedOut).IsEqualTo(0).Because($"completed={completed}");
        _ = await Assert.That(completed).IsGreaterThanOrEqualTo(CutsRequired);
        _ = await Assert.That(capturedWithPending).IsEqualTo(0);
    }

    private static async Task WriteUntilStoppedAsync(AppliedKeys memory, DurableMutationExecutor executor, StallableJournal journal, string key, CancellationToken stopped)
    {
        while (!stopped.IsCancellationRequested)
            _ = await memory.PutAsync(executor, journal.Journal, key, CancellationToken.None);
    }
}
