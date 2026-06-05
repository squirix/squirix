namespace Squirix.Server.Storage.Journaling.Json;

internal sealed class ItemPair
{
    public byte[]? EntryJsonUtf8 { get; init; }

    public string Key { get; init; } = string.Empty;

    public string? Namespace { get; init; }
}
