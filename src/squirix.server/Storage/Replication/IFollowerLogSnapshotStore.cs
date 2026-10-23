using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Published snapshot surface coordinators use from the group snapshot store.</summary>
internal interface IFollowerLogSnapshotStore
{
    /// <summary>Gets a value indicating whether a published snapshot currently exists.</summary>
    /// <remarks>
    /// Implementations may query the filesystem on every read, so treat this as an I/O operation rather than a cached
    /// value. The result is advisory: a concurrent publication can change it immediately after the read returns.
    /// </remarks>
    bool SnapshotExists { get; }

    /// <summary>Gets the path of the first published snapshot slot file.</summary>
    string SnapshotPath { get; }

    /// <summary>Tells, without writing, whether the published file image of a snapshot stays within the configured maximum size.</summary>
    /// <param name="snapshot">The snapshot to check.</param>
    /// <returns><see langword="true" /> when <see cref="PublishAsync" /> would not refuse the snapshot for its size.</returns>
    bool Fits(GroupSnapshot snapshot);

    /// <summary>Durably publishes a snapshot into the two in-place slot files.</summary>
    /// <param name="snapshot">The snapshot to persist.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the snapshot is durably published.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the snapshot committed outcomes are null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the snapshot violates its boundary invariants.</exception>
    /// <exception cref="GroupSnapshotTooLargeException">Thrown before anything is written when the snapshot exceeds the configured maximum size.</exception>
    /// <remarks>
    /// Callers must serialize publication per instance. Implementations write the slot files in place, so concurrent calls
    /// can fail with an <see cref="IOException" /> or overwrite each other.
    /// </remarks>
    Task PublishAsync(GroupSnapshot snapshot, CancellationToken cancellationToken);

    /// <summary>Reads and validates the published snapshot file, if any.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The validated snapshot, or <see langword="null" /> when no snapshot is published.</returns>
    /// <exception cref="InvalidDataException">Thrown when the published snapshot fails structural or CRC validation.</exception>
    Task<GroupSnapshot?> ReadPublishedAsync(CancellationToken cancellationToken);
}
