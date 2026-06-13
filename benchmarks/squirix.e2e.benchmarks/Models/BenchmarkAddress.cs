using System.Diagnostics.CodeAnalysis;

// ReSharper disable NotAccessedPositionalProperty.Global
namespace Squirix.E2EBenchmarks.Models;

/// <summary>
/// Address value used by custom benchmark records.
/// </summary>
/// <param name="City">City name.</param>
/// <param name="Street">Street name.</param>
/// <param name="PostalCode">Postal code.</param>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark data record is serialized in public benchmark payloads.")]
public sealed record BenchmarkAddress(string City, string Street, string PostalCode);
