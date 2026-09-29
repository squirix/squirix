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
    internal static IJournalCoordinator Create(PersistenceOptions persistence, State manifest, Ledger store, AsyncManualResetEvent gate, ILogger? recoveryLog = null)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        JournalRecoveryScan.DeleteOrphanedRollTempFiles(persistence.DataDir);
        var repairs = JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(manifest, persistence);
        if (repairs.Count > 0)
            LogRepairs(repairs, recoveryLog ?? LogManager.GetLogger("Squirix.Server.Storage.Journaling.JournalCoordinatorFactory"));

        return new JournalCoordinator(persistence, manifest, store, gate);
    }

    private static void LogRepairs(IReadOnlyList<JournalRepair> repairs, ILogger log)
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
