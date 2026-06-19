using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Encodes and decodes journal frame bodies for a specific on-disk format version.</summary>
internal interface IJournalFrameCodec
{
    byte FileVersion { get; }

    int Encode(JournalRecord record, Span<byte> destination);

    JournalRecord Decode(ReadOnlySpan<byte> frameBody);
}
