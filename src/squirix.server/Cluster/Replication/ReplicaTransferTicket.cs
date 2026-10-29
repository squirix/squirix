using System;
using System.Buffers.Binary;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The state of the one state transfer a group holds: who sends it, where it is, and the snapshot image received so far.</summary>
/// <remarks>
/// The message handlers change a ticket only while they hold the install gate of the group. <see cref="Busy" /> and <see cref="LastActivity" />
/// are also read by the idle timer, so the slot reads and writes those two under its lock.
/// </remarks>
internal sealed class ReplicaTransferTicket
{
    private byte[]? _image;
    private int _received;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTransferTicket" /> class.</summary>
    /// <param name="header">Who sends the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at.</param>
    /// <param name="startedAt">The timestamp of the begin on the clock of the receiver.</param>
    internal ReplicaTransferTicket(in ReplicaTransferHeader header, in ReplicaTransferBaseline baseline, long startedAt)
    {
        Header = header;
        Baseline = baseline with { Fingerprint = baseline.Fingerprint.ToArray() };
        StartedAt = startedAt;
        LastActivity = startedAt;
    }

    /// <summary>Gets the point the transfer is taken at; the fingerprint is a copy owned by the ticket.</summary>
    internal ReplicaTransferBaseline Baseline { get; }

    /// <summary>Gets or sets a value indicating whether a message of the transfer is being handled.</summary>
    internal bool Busy { get; set; }

    /// <summary>Gets or sets the number of entries the finished transfer wrote.</summary>
    internal ulong Entries { get; set; }

    /// <summary>Gets the sequence number the next message must carry; the begin was zero.</summary>
    internal int ExpectedSequence { get; private set; } = 1;

    /// <summary>Gets who sends the transfer.</summary>
    internal ReplicaTransferHeader Header { get; }

    /// <summary>Gets a value indicating whether the snapshot image is received completely.</summary>
    internal bool IsImageComplete => _image != null && _received == _image.Length;

    /// <summary>Gets or sets a value indicating whether the copy was marked incomplete: from then on the transfer has changed the copy.</summary>
    internal bool InstallBegun { get; set; }

    /// <summary>Gets the leader that sends the transfer, as the group log takes it: its term, its node and whether it checked the term ceiling.</summary>
    internal StateInstallLeader Leader => new(Header.LeaderTerm, Header.LeaderNodeId, Baseline.CeilingChecked);

    /// <summary>Gets or sets the number of entries of the batch being resent that the sink already wrote; zero when no batch is half written.</summary>
    /// <remarks>
    /// A batch the node journal accepted only in part is refused as busy and sent again unchanged, so the receiver continues from this entry. It is
    /// changed only while the message gate of the group is held.
    /// </remarks>
    internal int ResumeAt { get; set; }

    /// <summary>Gets or sets the timestamp of the last accepted message.</summary>
    internal long LastActivity { get; set; }

    /// <summary>Gets or sets the step the transfer expects next.</summary>
    internal ReplicaTransferPhase Phase { get; set; }

    /// <summary>Gets the decoded snapshot; meaningful once <see cref="TryDecode" /> succeeded.</summary>
    internal GroupSnapshot Snapshot { get; private set; }

    /// <summary>Gets the timestamp of the begin.</summary>
    internal long StartedAt { get; }

    /// <summary>Counts an accepted message.</summary>
    internal void Advance() => ExpectedSequence++;

    /// <summary>Drops the snapshot image buffer.</summary>
    internal void ReleaseImage() => _image = null;

    /// <summary>Appends a chunk of the snapshot image.</summary>
    /// <param name="offset">The offset of the chunk in the image; it must be the number of bytes received so far.</param>
    /// <param name="chunk">The chunk.</param>
    /// <param name="overflow">Whether the chunk reaches beyond the declared image length.</param>
    /// <returns><see langword="true" /> when the chunk was appended; otherwise nothing changed.</returns>
    internal bool TryReceive(long offset, ReadOnlySpan<byte> chunk, out bool overflow)
    {
        _image ??= new byte[Baseline.SnapshotBytes];
        overflow = chunk.Length > _image.Length - _received;
        if (offset != _received || overflow)
            return false;

        chunk.CopyTo(_image.AsSpan(_received));
        _received += chunk.Length;
        return true;
    }

    /// <summary>Decodes the received image and checks it against what the begin declared.</summary>
    /// <param name="maxSnapshotBytes">The largest image the decoder accepts.</param>
    /// <param name="failure">The validation failure, or <see langword="null" /> on success.</param>
    /// <returns><see langword="true" /> when the image is valid and names the declared group, index, term and checksum.</returns>
    internal bool TryDecode(int maxSnapshotBytes, out string? failure)
    {
        failure = "incomplete-snapshot-image";
        if (_image == null || !IsImageComplete)
            return false;

        if (!GroupSnapshotStore.TryDecodePublished(_image, maxSnapshotBytes, out var snapshot, out _, out failure))
            return false;

        var checksum = BinaryPrimitives.ReadUInt32LittleEndian(_image.AsSpan(_image.Length - sizeof(uint)));
        failure = true switch
        {
            _ when checksum != Baseline.SnapshotChecksum => "snapshot-checksum-declaration-mismatch",
            _ when snapshot.LastIncludedIndex != Baseline.Index => "snapshot-index-mismatch",
            _ when snapshot.LastIncludedTerm != Baseline.IndexTerm => "snapshot-index-term-mismatch",
            _ when !string.Equals(snapshot.GroupId, Header.GroupId, StringComparison.Ordinal) => "snapshot-group-mismatch",
            _ => null,
        };
        if (failure != null)
            return false;

        Snapshot = snapshot;
        return true;
    }
}
