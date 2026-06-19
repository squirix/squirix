namespace Squirix.Server.Storage.Journaling.Json;

internal sealed class TouchExpirationOp
{
    public long ExpiresUnixMs { get; init; }

    public string Key { get; init; } = string.Empty;

    public string? Namespace { get; init; }
}
