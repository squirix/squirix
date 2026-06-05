namespace Squirix.Server.Storage.Journaling.Json;

internal abstract class OpBase
{
    public string? OperationId { get; init; }
}
