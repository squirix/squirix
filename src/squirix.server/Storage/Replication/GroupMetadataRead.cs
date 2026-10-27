using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>The result of reading a group metadata file.</summary>
/// <param name="Status">Whether the file holds a valid record.</param>
/// <param name="Metadata">The newest valid record; default unless <paramref name="Status" /> is <see cref="GroupMetadataReadStatus.Valid" />.</param>
/// <param name="Slot">The index of the slot holding <paramref name="Metadata" />.</param>
/// <param name="Sequence">The sequence number of <paramref name="Metadata" />.</param>
[Immutable]
internal readonly record struct GroupMetadataRead(GroupMetadataReadStatus Status, GroupLogMetadata Metadata, int Slot, ulong Sequence);
