namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>Durability mode exposed by the E2E benchmark harness.</summary>
public enum DurabilityMode
{
    /// <summary>In-memory cache without journal/snapshot persistence.</summary>
    Ephemeral = 0,

    /// <summary>journal/snapshot persistence enabled.</summary>
    Persistence = 1,
}
