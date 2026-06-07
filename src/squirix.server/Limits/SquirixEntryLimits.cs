namespace Squirix.Server.Limits;

/// <summary>
/// Fixed v0.1.x entry and gRPC transport size limits (not user-configurable yet).
/// Transport limits are intentionally larger than <see cref="MaxEntrySizeBytes" /> so Squirix returns a controlled error first.
/// </summary>
internal static class SquirixEntryLimits
{
    public const int MaxEntrySizeBytes = 4 * 1024 * 1024;

    public const int GrpcMaxReceiveMessageSizeBytes = 8 * 1024 * 1024;

    public const int GrpcMaxSendMessageSizeBytes = 8 * 1024 * 1024;
}
