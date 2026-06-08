namespace Squirix.Internal.Limits;

/// <summary>
/// Client gRPC transport limits aligned with server <c>Squirix.Server.Limits.SquirixEntryLimits</c>.
/// </summary>
internal static class SquirixClientGrpcLimits
{
    public const int MaxReceiveMessageSizeBytes = 8 * 1024 * 1024;

    public const int MaxSendMessageSizeBytes = 8 * 1024 * 1024;
}
