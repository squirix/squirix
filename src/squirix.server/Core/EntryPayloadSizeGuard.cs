using System;
using Squirix.Server.Errors;

namespace Squirix.Server.Core;

/// <summary>Validates journal entry payload size against <see cref="EntryLimits.MaxEntrySizeBytes" />.</summary>
internal static class EntryPayloadSizeGuard
{
    internal static void EnsureEntryBytesWithinLimit(ReadOnlySpan<byte> entryBytes)
    {
        if (entryBytes.Length > EntryLimits.MaxEntrySizeBytes)
            throw ServerOpContract.PayloadTooLarge(EntryLimits.MaxEntrySizeBytes);
    }

    internal static void EnsureLengthWithinLimit(int encodedLength)
    {
        if (encodedLength > EntryLimits.MaxEntrySizeBytes)
            throw ServerOpContract.PayloadTooLarge(EntryLimits.MaxEntrySizeBytes);
    }
}
