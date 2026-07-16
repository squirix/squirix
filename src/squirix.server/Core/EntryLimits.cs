namespace Squirix.Server.Core;

/// <summary>
/// Fixed v0.1.x entry and gRPC transport size limits (not user-configurable yet).
/// Transport limits are intentionally larger than <see cref="MaxEntrySizeBytes" /> so Squirix returns a controlled error first.
/// </summary>
internal static class EntryLimits
{
    /// <summary>Maximum gRPC receive message size in bytes (8 MiB).</summary>
    public const int GrpcMaxReceiveMessageSizeBytes = 8 * 1024 * 1024;

    /// <summary>Maximum gRPC send message size in bytes (8 MiB).</summary>
    public const int GrpcMaxSendMessageSizeBytes = 8 * 1024 * 1024;

    /// <summary>Maximum encoded cache entry payload size in bytes (4 MiB).</summary>
    public const int MaxEntrySizeBytes = 4 * 1024 * 1024;

    /// <summary>Maximum number of tags on a cache entry.</summary>
    public const int MaxEntryTagCount = 32;

    /// <summary>Maximum UTF-8 byte length of a cache entry tag key.</summary>
    public const int MaxEntryTagKeyUtf8Bytes = 256;

    /// <summary>Maximum UTF-8 byte length of a cache entry tag value.</summary>
    public const int MaxEntryTagValueUtf8Bytes = 1024;
}
