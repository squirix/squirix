using System;
using System.Collections.Generic;

// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Squirix.E2EBenchmarks.Models;

/// <summary>
/// Mutable custom class used by nested class serialization benchmarks.
/// </summary>
public sealed class BenchmarkOrder
{
    /// <summary>
    /// Gets or sets the order identifier.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the customer identifier.
    /// </summary>
    public string CustomerId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the order lines.
    /// </summary>
    public List<BenchmarkOrderLine> Lines { get; set; } = [];

    /// <summary>
    /// Gets or sets diagnostic tags.
    /// </summary>
    public Dictionary<string, string> Tags { get; set; } = [];

    /// <summary>
    /// Gets or sets the creation timestamp.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }
}
