using System;
using System.IO;
using Squirix.Server.Cluster.Replication;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Wire.Replication;

/// <summary>A batch in sender order. Common names and absent deadlines can be encoded once per batch.</summary>
internal sealed class ReplicaTransferEntries : WireMessage
{
    internal static WireParser<ReplicaTransferEntries> Parser { get; } = new(Read);

    internal ReadOnlyMemory<ReplicaStateEntry> Entries { get; init; }

    internal static ReplicaTransferEntries Read(ref WireReader reader)
    {
        var packed = reader.Unsigned();
        var count = packed >> 2;
        var shared = (packed & 1) != 0;
        var noExpiry = (packed & 2) != 0;
        if (count > ReplicaTransferOptions.MaxMessageEntries || (shared && count < 2) || (noExpiry && count == 0))
            throw new InvalidDataException("Invalid state transfer entry count.");

        var name = shared ? reader.Text() : null;
        var minimumBytes = 2 + (shared ? 0 : 1) + (noExpiry ? 0 : 1);
        if (count > ulong.CreateChecked(reader.Remaining / minimumBytes))
            throw new InvalidDataException("Incomplete state transfer entries.");

        var entries = new ReplicaStateEntry[int.CreateChecked(count)];
        for (var i = 0; i < entries.Length; i++)
            entries[i] = new ReplicaStateEntry(name ?? reader.Text(), reader.Bytes(out _), reader.Bytes(out _), noExpiry ? 0 : reader.Signed());
        return new ReplicaTransferEntries { Entries = entries };
    }

    internal override void Encode(ref WireWriter writer)
    {
        var entries = Entries.Span;
        if (entries.Length > ReplicaTransferOptions.MaxMessageEntries)
            throw new InvalidDataException("Too many state transfer entries.");

        var shared = entries.Length > 1;
        var noExpiry = !entries.IsEmpty;
        foreach (ref readonly var entry in entries)
        {
            shared = shared && string.Equals(entry.CacheName, entries[0].CacheName, StringComparison.Ordinal);
            noExpiry &= entry.ExpiresUtcTicks == 0;
            if (!shared && !noExpiry)
                break;
        }

        writer.Unsigned((ulong.CreateChecked(entries.Length) << 2) | (shared ? 1UL : 0UL) | (noExpiry ? 2UL : 0UL));
        if (shared)
            writer.Text(entries[0].CacheName);
        foreach (ref readonly var entry in entries)
        {
            if (!shared)
                writer.Text(entry.CacheName);
            writer.Bytes(entry.Key);
            writer.Bytes(entry.Payload);
            if (!noExpiry)
                writer.Signed(entry.ExpiresUtcTicks);
        }
    }
}
