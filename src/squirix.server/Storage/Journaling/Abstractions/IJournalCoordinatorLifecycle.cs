using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Startup, sequencing, and append notifications for the journal coordinator.</summary>
internal interface IJournalCoordinatorLifecycle
{
    event EventHandler? OnAppended;

    int CurrentSegmentIndex { get; }

    bool HasFlushLoopFailure { get; }

    bool IsJournalGroupCommitEnabled { get; }

    ulong NextSequence { get; }

    ValueTask WaitForStartupAsync(CancellationToken cancellationToken);
}
