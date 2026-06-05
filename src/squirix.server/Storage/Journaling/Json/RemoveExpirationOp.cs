namespace Squirix.Server.Storage.Journaling.Json;

internal sealed class RemoveExpirationOp
{
    public string Key { get; init; } = string.Empty;

    public string? Namespace { get; init; }
}
