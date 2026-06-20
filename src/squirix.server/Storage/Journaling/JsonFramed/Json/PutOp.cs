namespace Squirix.Server.Storage.Journaling.JsonFramed.Json;

internal sealed class PutOp : OpBase
{
    public ItemPair Item { get; init; } = new();
}
