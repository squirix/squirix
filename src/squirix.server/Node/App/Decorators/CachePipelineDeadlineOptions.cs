using System;
using JetBrains.Annotations;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Optional logical-cache pipeline deadline budget applied by <see cref="DeadlineCacheDecorator{T}" />.</summary>
/// <remarks>
/// Transport-level retries and budgets remain governed by cluster call policy and RPC deadline context.
/// When this option is unset, the decorator is a pass-through and does not introduce cancellation or timeouts.
/// </remarks>
[Immutable]
internal sealed class CachePipelineDeadlineOptions
{
    /// <summary>
    /// Gets the maximum duration for a single logical cache operation (excluding long-lived watch streams).
    /// When null or non-positive, pipeline deadlines are disabled.
    /// </summary>
    internal TimeSpan? DefaultOperationTimeout
    {
        get;
        [UsedImplicitly]
        init;
    }
}
