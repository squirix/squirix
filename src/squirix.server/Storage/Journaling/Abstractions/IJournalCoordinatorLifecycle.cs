using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Startup, sequencing, and append notifications for the journal coordinator.</summary>
internal interface IJournalCoordinatorLifecycle
{
    event EventHandler? OnAppended;

    int CurrentSegmentIndex { get; }

    bool IsJournalGroupCommitEnabled { get; }

    /// <summary>Gets the next sequence to allocate; the last allocated sequence is this value minus one.</summary>
    ulong NextSequence { get; }

    /// <summary>Returns the first failure latched by the journal pipeline; once set, the node cannot commit until restart.</summary>
    /// <returns>The latched failure, or <see langword="null" /> while the pipeline is healthy.</returns>
    Exception? GetJournalThreadFailure();

    ValueTask WaitForStartupAsync(CancellationToken cancellationToken);
}
