using System.Collections.Frozen;
using System.Text;
using Squirix.Server.Errors;

namespace Squirix.Server.Core;

/// <summary>Validates cache entry tag count and UTF-8 key/value sizes before persistence.</summary>
internal static class EntryTagsGuard
{
    /// <summary>Throws when <paramref name="tags" /> exceed entry tag limits.</summary>
    /// <param name="tags">Optional entry tags.</param>
    /// <exception cref="SquirixException">Thrown when tag count or UTF-8 sizes exceed limits.</exception>
    internal static void EnsureWithinLimits(FrozenDictionary<string, string>? tags)
    {
        if (tags is null || tags.Count is 0)
            return;

        if (tags.Count > EntryLimits.MaxEntryTagCount)
            throw ServerOpContract.EntryTagCountExceeded(EntryLimits.MaxEntryTagCount);

        foreach (var pair in tags)
        {
            if (Encoding.UTF8.GetByteCount(pair.Key) > EntryLimits.MaxEntryTagKeyUtf8Bytes)
                throw ServerOpContract.EntryTagKeyTooLarge(EntryLimits.MaxEntryTagKeyUtf8Bytes);

            if (Encoding.UTF8.GetByteCount(pair.Value) > EntryLimits.MaxEntryTagValueUtf8Bytes)
                throw ServerOpContract.EntryTagValueTooLarge(EntryLimits.MaxEntryTagValueUtf8Bytes);
        }
    }
}
