namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Centralized namespace allowlists for naming-convention architecture rules.</summary>
internal static class ArchitectureAllowlists
{
    /// <summary>
    /// Exact namespaces where server <c>*Options</c> types are permitted to reside.
    /// </summary>
    public static readonly string[] ServerOptionsTypeNamespaces =
    [
        ServerArchitectureNamespaces.Root,
        "Squirix.Server.Core",
        $"{ServerArchitectureNamespaces.Node}.Backpressure",
        $"{ServerArchitectureNamespaces.Node}.MemoryPressure",
        $"{ServerArchitectureNamespaces.Node}.Services",
        $"{ServerArchitectureNamespaces.Node}.Hosting",
        $"{ServerArchitectureNamespaces.Node}.Hosting.Security",
        $"{ServerArchitectureNamespaces.Node}.Observability.Metrics",
        $"{ServerArchitectureNamespaces.Node}.App.Decorators",
        ServerArchitectureNamespaces.Storage,
        $"{ServerArchitectureNamespaces.Storage}.Snapshot",
        $"{ServerArchitectureNamespaces.Storage}.Journaling",
        $"{ServerArchitectureNamespaces.Storage}.Journaling.Compaction",
        $"{ServerArchitectureNamespaces.Cluster}.Transport",
    ];

    /// <summary>
    /// Exact namespaces where <c>*Service</c> types are permitted to reside.
    /// </summary>
    public static readonly string[] ServiceTypeNamespaces =
    [
        $"{ServerArchitectureNamespaces.Node}.Services",
        ServerArchitectureNamespaces.Cluster,
        $"{ServerArchitectureNamespaces.Node}.Context",
        "Squirix.Transport.Grpc",
    ];

    /// <summary>
    /// Exact namespaces passed to architecture rules for <c>*Validator</c> types.
    /// Excludes legacy <c>Squirix</c> and <c>Squirix.Core</c> roots that remain documented elsewhere.
    /// </summary>
    public static readonly string[] ValidatorTypeArchitectureNamespaces =
    [
        ServerArchitectureNamespaces.Root,
        "Squirix.Server.Core",
        $"{ServerArchitectureNamespaces.Node}.Hosting",
        $"{ServerArchitectureNamespaces.Node}.Bootstrap",
        $"{ServerArchitectureNamespaces.Node}.App",
        $"{ServerArchitectureNamespaces.Node}.App.Decorators.Validation",
        $"{ServerArchitectureNamespaces.Cluster}.Transport",
    ];
}
