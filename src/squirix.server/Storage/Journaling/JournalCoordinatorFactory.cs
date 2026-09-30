using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalCoordinator" /> instances.</summary>
internal static class JournalCoordinatorFactory
{
    /// <summary>Creates the coordinator and hands back the startup repairs so the caller can log them.</summary>
    /// <param name="persistence">Persistence options.</param>
    /// <param name="manifest">The current manifest.</param>
    /// <param name="store">The manifest ledger.</param>
    /// <param name="gate">The startup gate.</param>
    /// <param name="loggerFactory">Creates the loggers of the coordinator and its components.</param>
    /// <param name="repairs">Every repair startup recovery applied to a segment file; empty when nothing changed.</param>
    /// <returns>The coordinator.</returns>
    internal static IJournalCoordinator Create(PersistenceOptions persistence, State manifest, Ledger store, AsyncManualResetEvent gate, ILoggerFactory loggerFactory, out IReadOnlyList<JournalRepair> repairs)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        JournalRecoveryScan.DeleteOrphanedRollTempFiles(persistence.DataDir);
        repairs = JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(manifest, persistence);
        return new JournalCoordinator(persistence, manifest, store, gate, loggerFactory);
    }
}
