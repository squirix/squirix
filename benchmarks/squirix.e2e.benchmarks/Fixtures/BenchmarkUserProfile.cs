using System;
using System.Collections.Generic;
using Squirix.Attributes;

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
[Immutable]
public sealed record BenchmarkUserProfile(
    long Id,
    string Name,
    string? Email,
    BenchmarkAddress Address,
    IReadOnlyList<string> Roles,
    DateTimeOffset CreatedAt,
    BenchmarkUserStatus Status);
