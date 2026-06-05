namespace Squirix.Server.Node.Services;

internal sealed record InsertOutcome : IdempotencyOutcome
{
    public static readonly InsertOutcome Instance = new();
}
