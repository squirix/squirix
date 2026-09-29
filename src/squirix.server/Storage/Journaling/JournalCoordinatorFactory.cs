using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalCoordinator" /> instances.</summary>
internal static class JournalCoordinatorFactory
{
    internal static IJournalCoordinator Create(PersistenceOptions persistence, State manifest, Ledger store, AsyncManualResetEvent gate, ILogger log)
    {
        ArgumentNullException.ThrowIfNull(log);
        var coordinator = CreateReporting(persistence, manifest, store, gate, out var repairs);
        LogRepairs(repairs, log);
        return coordinator;
    }

    /// <summary>Creates the coordinator and hands back the startup repairs so the caller can log them once it has a logger.</summary>
    /// <param name="persistence">Persistence options.</param>
    /// <param name="manifest">The current manifest.</param>
    /// <param name="store">The manifest ledger.</param>
    /// <param name="gate">The startup gate.</param>
    /// <param name="repairs">Every repair startup recovery applied to a segment file; empty when nothing changed.</param>
    /// <returns>The coordinator.</returns>
    internal static IJournalCoordinator CreateReporting(PersistenceOptions persistence, State manifest, Ledger store, AsyncManualResetEvent gate, out IReadOnlyList<JournalRepair> repairs)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        JournalRecoveryScan.DeleteOrphanedRollTempFiles(persistence.DataDir);
        repairs = JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(manifest, persistence);
        return new JournalCoordinator(persistence, manifest, store, gate);
    }

    internal static void LogRepairs(IReadOnlyList<JournalRepair> repairs, ILogger log)
    {
        for (var i = 0; i < repairs.Count; i++)
        {
            var repair = repairs[i];
            switch (repair.Kind)
            {
                case JournalRepairKind.HeaderRestored:
                    LogManager.JournalHeaderRestored(log, repair.Path, repair.OriginalLength, repair.DiscardedBytes);
                    break;
                case JournalRepairKind.TornCreationHeaderRewritten:
                    LogManager.JournalTornCreationRewritten(log, repair.Path, repair.OriginalLength, repair.DiscardedBytes);
                    break;
                case JournalRepairKind.TornTailTruncated:
                    LogManager.JournalTornTailTruncated(log, repair.Path, repair.OriginalLength, repair.DiscardedBytes);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(repairs), repair.Kind, "Unsupported journal repair kind.");
            }
        }
    }
}
