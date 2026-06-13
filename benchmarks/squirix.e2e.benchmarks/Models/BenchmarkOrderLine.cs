using System.Diagnostics.CodeAnalysis;

// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Squirix.E2EBenchmarks.Models;

/// <summary>
/// Order line used by mutable custom benchmark classes.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark data model is serialized in public benchmark payloads.")]
public sealed class BenchmarkOrderLine
{
    /// <summary>
    /// Gets or sets the unit price.
    /// </summary>
    public decimal Price { get; set; }

    /// <summary>
    /// Gets or sets the quantity.
    /// </summary>
    public int Quantity { get; set; }

    /// <summary>
    /// Gets or sets the SKU.
    /// </summary>
    public string Sku { get; set; } = string.Empty;
}
