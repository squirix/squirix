using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using JetBrains.Annotations;

// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Squirix.E2EBenchmarks.Models;

/// <summary>
/// Custom class used by nested class serialization benchmarks.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark data model is serialized in public benchmark payloads.")]
public sealed class BenchmarkOrder
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BenchmarkOrder" /> class.
    /// </summary>
    [UsedImplicitly]
    public BenchmarkOrder()
    {
        Lines = [];
        Tags = new Dictionary<string, string>(StringComparer.Ordinal);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="BenchmarkOrder" /> class.
    /// </summary>
    /// <param name="id">Order identifier.</param>
    /// <param name="customerId">Customer identifier.</param>
    /// <param name="createdAt">Creation timestamp.</param>
    /// <param name="lines">Order lines.</param>
    /// <param name="tags">Diagnostic tags.</param>
    public BenchmarkOrder(string id, string customerId, DateTimeOffset createdAt, IReadOnlyList<BenchmarkOrderLine> lines, IReadOnlyDictionary<string, string> tags)
    {
        ArgumentNullException.ThrowIfNull(lines);
        ArgumentNullException.ThrowIfNull(tags);

        Id = id;
        CustomerId = customerId;
        CreatedAt = createdAt;
        Lines = lines;
        Tags = new Dictionary<string, string>(tags, StringComparer.Ordinal);
    }

    /// <summary>
    /// Gets or sets the creation timestamp.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>
    /// Gets or sets the customer identifier.
    /// </summary>
    public string CustomerId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the order identifier.
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Gets the order lines.
    /// </summary>
    [JsonInclude]
    public IReadOnlyList<BenchmarkOrderLine> Lines { get; private set; }

    /// <summary>
    /// Gets diagnostic tags.
    /// </summary>
    [JsonInclude]
    public IReadOnlyDictionary<string, string> Tags { get; private set; }
}
