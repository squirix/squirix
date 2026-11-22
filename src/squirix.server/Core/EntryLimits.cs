using Squirix.Transport.Binary;

namespace Squirix.Server.Core;

/// <summary>Limits for cache entries and the short form of peer frames.</summary>
internal static class EntryLimits
{
    /// <summary>Maximum serialized value admitted on a replication-enabled node.</summary>
    internal const int MaxReplicatedValueBytes = 32 * 1024 * 1024;

    /// <summary>Maximum short-form peer receive message size in bytes (8 MiB).</summary>
    internal const int MaxReceiveMessageSizeBytes = 8 * 1024 * 1024;

    /// <summary>Maximum short-form peer send message size in bytes (8 MiB).</summary>
    internal const int MaxSendMessageSizeBytes = 8 * 1024 * 1024;

    /// <summary>Maximum number of tags on a cache entry.</summary>
    internal const int MaxEntryTagCount = 32;

    /// <summary>Maximum UTF-8 byte length of a cache entry tag key.</summary>
    internal const int MaxEntryTagKeyUtf8Bytes = 256;

    /// <summary>Maximum UTF-8 byte length of a cache entry tag value.</summary>
    internal const int MaxEntryTagValueUtf8Bytes = 1024;

    internal static int ValueBytes(bool replicated) => replicated ? MaxReplicatedValueBytes : Frames.MaxValueBytes;
}
