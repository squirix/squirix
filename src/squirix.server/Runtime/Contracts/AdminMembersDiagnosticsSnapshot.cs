namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Static cluster topology snapshot for admin REST endpoints.
/// </summary>
internal sealed class AdminMembersDiagnosticsSnapshot
{
    public required string[] Members { get; init; }

    public required int VirtualNodes { get; init; }
}
