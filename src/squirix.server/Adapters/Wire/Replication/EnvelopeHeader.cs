using System;
using System.IO;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Adapters.Wire.Replication;

/// <summary>Both peer identity claims survive decoding for independent authorization against the certificate.</summary>
internal sealed class EnvelopeHeader : WireMessage
{
    internal static WireParser<EnvelopeHeader> Parser { get; } = new(Read);

    internal uint SchemaVersion { get; set; }

    internal string GroupId { get; set; } = string.Empty;

    internal ReadOnlyMemory<byte> TopologyFingerprint { get; set; }

    internal ulong ConfigurationGeneration { get; set; }

    internal ulong Term { get; set; }

    internal string LeaderNodeId { get; set; } = string.Empty;

    internal string SenderNodeId { get; set; } = string.Empty;

    internal static EnvelopeHeader Read(ref WireReader reader)
    {
        var packed = reader.Unsigned();
        if ((packed >> 1) > uint.MaxValue)
            throw new InvalidDataException("Invalid replication schema version.");

        var header = new EnvelopeHeader
        {
            SchemaVersion = uint.CreateChecked(packed >> 1),
            GroupId = reader.Text(),
            TopologyFingerprint = reader.Bytes(out _),
            ConfigurationGeneration = reader.Unsigned(),
            Term = reader.Unsigned(),
            SenderNodeId = reader.Text(),
        };
        header.LeaderNodeId = (packed & 1) != 0 ? header.SenderNodeId : reader.Text();
        return header;
    }

    internal override void Encode(ref WireWriter writer)
    {
        var shared = string.Equals(LeaderNodeId, SenderNodeId, StringComparison.Ordinal);
        writer.Unsigned((ulong.CreateChecked(SchemaVersion) << 1) | (shared ? 1UL : 0UL));
        writer.Text(GroupId);
        writer.Bytes(TopologyFingerprint);
        writer.Unsigned(ConfigurationGeneration);
        writer.Unsigned(Term);
        writer.Text(SenderNodeId);
        if (!shared)
            writer.Text(LeaderNodeId);
    }
}
