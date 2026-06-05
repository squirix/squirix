namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Identifies layers in the hosted logical cache pipeline. Order is significant only when interpreted
/// through <see cref="CachePipelineDescriptor" />.
/// </summary>
internal enum CachePipelineLayer
{
    /// <summary>Gets the outermost logical boundary registered as <c>ILogicalNamespacedCache&lt;T&gt;</c>.</summary>
    Tracing,

    /// <summary>Gets the transport and domain error normalization layer.</summary>
    DomainErrorMapping,

    /// <summary>Gets the pipeline-wide deadline classification layer.</summary>
    Deadline,

    /// <summary>Gets the request validation layer.</summary>
    Validation,

    /// <summary>Gets the backpressure gate layer.</summary>
    Backpressure,

    /// <summary>Gets the logical operation metrics layer.</summary>
    Metrics,

    /// <summary>Gets the memory admission gate layer.</summary>
    MemoryAdmission,

    /// <summary>Gets the cluster routing layer.</summary>
    Clustered,

    /// <summary>Gets the local owner guard layer.</summary>
    OwnershipGuard,

    /// <summary>Gets the durable journal logging layer.</summary>
    JournalLogging,

    /// <summary>Gets the optional inner branch constructed inside the journal logging factory when memory accounting is enabled.</summary>
    MemoryAccounting,

    /// <summary>Gets the innermost local physical composition root over focused local-cache contracts.</summary>
    ClientCache,
}
