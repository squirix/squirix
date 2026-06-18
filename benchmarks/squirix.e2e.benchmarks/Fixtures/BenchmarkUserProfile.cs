using System;
using System.Diagnostics.CodeAnalysis;

// ReSharper disable NotAccessedPositionalProperty.Global
namespace Squirix.E2EBenchmarks.Fixtures;

/// <summary>Immutable custom record used by E2E serialization benchmarks.</summary>
/// <param name="Id">User identifier.</param>
/// <param name="Name">User display name.</param>
/// <param name="Email">Optional email address.</param>
/// <param name="Address">Nested address value.</param>
/// <param name="Roles">Role names.</param>
/// <param name="CreatedAt">Creation timestamp.</param>
/// <param name="Status">User status.</param>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Benchmark data record is serialized in public benchmark payloads.")]
[SuppressMessage("Usage", "CA1819:Properties should not return arrays", Justification = "Benchmark payload intentionally models an array property shape.")]
public sealed record BenchmarkUserProfile(
    long Id,
    string Name,
    string? Email,
    BenchmarkAddress Address,
    string[] Roles,
    DateTimeOffset CreatedAt,
    BenchmarkUserStatus Status)
{
    /// <summary>Initializes a new instance of the <see cref="BenchmarkUserProfile" /> class.</summary>
    /// <param name="id">User identifier.</param>
    /// <param name="name">User display name.</param>
    /// <param name="email">Optional email address.</param>
    /// <param name="address">Nested address value.</param>
    /// <param name="roles">Role names.</param>
    /// <param name="createdAt">Creation timestamp.</param>
    /// <param name="status">User status.</param>
    public BenchmarkUserProfile(
        long id,
        string name,
        string? email,
        BenchmarkAddress address,
        ReadOnlySpan<string> roles,
        DateTimeOffset createdAt,
        BenchmarkUserStatus status)
        : this(id, name, email, address, roles.ToArray(), createdAt, status)
    {
    }
}
