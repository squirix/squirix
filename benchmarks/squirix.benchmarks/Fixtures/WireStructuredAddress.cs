// ReSharper disable NotAccessedPositionalProperty.Global
namespace Squirix.Benchmarks.Fixtures;

/// <summary>Nested address value for structured wire allocation benchmarks.</summary>
/// <param name="City">City name.</param>
/// <param name="Street">Street name.</param>
/// <param name="PostalCode">Postal code.</param>
public sealed record WireStructuredAddress(string City, string Street, string PostalCode);
