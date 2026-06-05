namespace Squirix.Server.Storage.Journaling.Json;

internal sealed class RemoveOp
{
    public string Key { get; init; } = string.Empty;

    public string? Namespace { get; init; }
}
