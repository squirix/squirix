namespace Squirix.Server.Storage.Journaling.JsonFramed.Json;

internal abstract class OpBase
{
    public string? OperationId { get; init; }
}
