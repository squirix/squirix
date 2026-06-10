namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Centralized namespace allowlists for naming-convention architecture rules.
/// </summary>
public static class ArchitectureAllowlists
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
        ServerArchitectureNamespaces.Storage,
        $"{ServerArchitectureNamespaces.Storage}.Snapshot",
        $"{ServerArchitectureNamespaces.Storage}.Journaling",
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
    /// Exact namespaces where <c>*Validator</c> types are permitted to reside.
    /// </summary>
    public static readonly string[] ValidatorTypeNamespaces =
    [
        ServerArchitectureNamespaces.Root,
        "Squirix",
        "Squirix.Core",
        "Squirix.Server.Core",
        $"{ServerArchitectureNamespaces.Node}.Hosting",
        $"{ServerArchitectureNamespaces.Node}.App",
        $"{ServerArchitectureNamespaces.Node}.App.Decorators.Validation",
    ];
}
