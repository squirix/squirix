// ReSharper disable NotAccessedPositionalProperty.Global
namespace Squirix.E2EBenchmarks.Models;

/// <summary>
/// Address value used by custom benchmark records.
/// </summary>
/// <param name="City">City name.</param>
/// <param name="Street">Street name.</param>
/// <param name="PostalCode">Postal code.</param>
public sealed record BenchmarkAddress(string City, string Street, string PostalCode);
