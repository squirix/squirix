using System.Collections.Generic;
using System.IO;
using Squirix.Server.Storage.Replication;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Adapters.Wire.Replication;

internal sealed class AppendReplicaEntriesRequest : WireMessage
{
    private const byte HeaderPresent = 1;
    private const byte Sequential = 2;
    private const byte PreviousTermFromHeader = 4;
    private const byte CommitAtPreviousIndex = 8;
    private const byte BatchTermFromHeader = 16;

    internal static WireParser<AppendReplicaEntriesRequest> Parser { get; } = new(Read);

    internal EnvelopeHeader? Header { get; set; }

    internal ulong PrevLogIndex { get; set; }

    internal ulong PrevLogTerm { get; set; }

    internal ulong LeaderCommitIndex { get; set; }

    internal IReadOnlyList<FollowerLogEntry> Entries { get; set; } = [];

    internal override void Encode(ref WireWriter writer)
    {
        var flags = GetFlags();
        writer.Byte(flags);
        Header?.Encode(ref writer);
        writer.Unsigned(PrevLogIndex);
        if ((flags & PreviousTermFromHeader) == 0)
            writer.Unsigned(PrevLogTerm);
        if ((flags & CommitAtPreviousIndex) == 0)
            writer.Unsigned(LeaderCommitIndex);
        writer.ItemCount(Entries.Count);
        var sequential = (flags & Sequential) != 0;
        if (sequential && (flags & BatchTermFromHeader) == 0)
            writer.Unsigned(Entries[0].Term);
        for (var i = 0; i < Entries.Count; i++)
        {
            var entry = Entries[i];
            if (!sequential)
            {
                writer.Unsigned(entry.LogIndex);
                writer.Unsigned(entry.Term);
            }

            writer.Bytes(entry.Payload);
        }
    }

    private static AppendReplicaEntriesRequest Read(ref WireReader reader)
    {
        var flags = reader.Flags(HeaderPresent | Sequential | PreviousTermFromHeader | CommitAtPreviousIndex | BatchTermFromHeader);
        var header = (flags & HeaderPresent) != 0 ? EnvelopeHeader.Read(ref reader) : null;
        var sequential = (flags & Sequential) != 0;
        if ((header == null && (flags & (PreviousTermFromHeader | BatchTermFromHeader)) != 0) || (!sequential && (flags & BatchTermFromHeader) != 0))
            throw new InvalidDataException("Invalid replica append term flags.");

        var previous = reader.Unsigned();
        var previousTerm = (flags & PreviousTermFromHeader) != 0 ? header!.Term : reader.Unsigned();
        var commit = (flags & CommitAtPreviousIndex) != 0 ? previous : reader.Unsigned();
        return new AppendReplicaEntriesRequest
        {
            Header = header,
            PrevLogIndex = previous,
            PrevLogTerm = previousTerm,
            LeaderCommitIndex = commit,
            Entries = ReadEntries(ref reader, flags, previous, header),
        };
    }

    private static FollowerLogEntry[] ReadEntries(ref WireReader reader, int flags, ulong previous, EnvelopeHeader? header)
    {
        var count = reader.ItemCount();
        var sequential = (flags & Sequential) != 0;
        if (sequential && (count == 0 || ulong.CreateChecked(count) > ulong.MaxValue - previous))
            throw new InvalidDataException("Invalid sequential replica log range.");

        ulong term = 0;
        if (sequential)
            term = (flags & BatchTermFromHeader) != 0 ? header!.Term : reader.Unsigned();
        if (count > reader.Remaining / (sequential ? 1 : 3))
            throw new InvalidDataException("Incomplete replica log entries.");

        var entries = count == 0 ? [] : new FollowerLogEntry[count];
        var index = previous;
        for (var i = 0; i < count; i++)
            entries[i] = new FollowerLogEntry(sequential ? ++index : reader.Unsigned(), sequential ? term : reader.Unsigned(), reader.Bytes(out _));
        return entries;
    }

    private byte GetFlags()
    {
        var flags = Header == null ? byte.MinValue : HeaderPresent;
        if (Header?.Term == PrevLogTerm)
            flags |= PreviousTermFromHeader;
        if (LeaderCommitIndex == PrevLogIndex)
            flags |= CommitAtPreviousIndex;
        if (Entries.Count == 0 || ulong.CreateChecked(Entries.Count) > ulong.MaxValue - PrevLogIndex)
            return flags;

        var index = PrevLogIndex;
        var term = Entries[0].Term;
        for (var i = 0; i < Entries.Count; i++)
        {
            var entry = Entries[i];
            if (entry.LogIndex != ++index || entry.Term != term)
                return flags;
        }

        flags |= Sequential;
        if (Header?.Term == term)
            flags |= BatchTermFromHeader;
        return flags;
    }
}
