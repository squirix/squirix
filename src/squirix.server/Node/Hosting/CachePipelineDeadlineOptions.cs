using System;
using JetBrains.Annotations;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Cluster.Reliability;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Optional logical-cache pipeline deadline budget applied by <see cref="DeadlineCacheDecorator{T}" />.
/// </summary>
/// <remarks>
/// Transport-level retries and budgets remain governed by <see cref="CallPolicy" /> and <see cref="RpcDeadlineContext" />.
/// When this option is unset, the decorator is a pass-through and does not introduce cancellation or timeouts.
/// </remarks>
internal sealed class CachePipelineDeadlineOptions
{
    /// <summary>
    /// Gets the maximum duration for a single logical cache operation (excluding long-lived watch streams).
    /// When null or non-positive, pipeline deadlines are disabled.
    /// </summary>
    public TimeSpan? DefaultOperationTimeout
    {
        get;
        [UsedImplicitly]
        init;
    }
}
