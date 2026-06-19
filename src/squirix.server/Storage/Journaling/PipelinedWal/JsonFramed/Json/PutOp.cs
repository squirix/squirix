namespace Squirix.Server.Storage.Journaling.Json;

internal sealed class PutOp : OpBase
{
    public ItemPair Item { get; init; } = new();
}
