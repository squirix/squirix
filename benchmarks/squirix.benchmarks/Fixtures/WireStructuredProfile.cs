using System;
using System.Collections.Generic;
using System.Globalization;

// ReSharper disable NotAccessedPositionalProperty.Global
namespace Squirix.Benchmarks.Fixtures;

/// <summary>Structured POCO used by in-process wire codec allocation benchmarks.</summary>
/// <param name="Id">User identifier.</param>
/// <param name="Name">User display name.</param>
/// <param name="Email">Optional email address.</param>
/// <param name="Address">Nested address value.</param>
/// <param name="Roles">Role names.</param>
/// <param name="CreatedAt">Creation timestamp.</param>
/// <param name="Status">User status.</param>
public sealed record WireStructuredProfile(
    long Id,
    string Name,
    string? Email,
    WireStructuredAddress Address,
    IReadOnlyList<string> Roles,
    DateTimeOffset CreatedAt,
    WireStructuredStatus Status)
{
    private static readonly DateTimeOffset BaseInstant = new(2026, 6, 6, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Creates a deterministic profile for benchmark setup and hot paths.</summary>
    /// <param name="index">Seed index for stable field values.</param>
    /// <returns>Structured profile instance.</returns>
    internal static WireStructuredProfile Create(int index) => new(
        index,
        $"User {index.ToString("D8", CultureInfo.InvariantCulture)}",
        $"user{index.ToString("D8", CultureInfo.InvariantCulture)}@example.test",
        new WireStructuredAddress("Seattle", "Pine Street", (98000 + (index % 100)).ToString(CultureInfo.InvariantCulture)),
        ["reader", "writer"],
        BaseInstant.AddMinutes(index),
        index % 17 is 0 ? WireStructuredStatus.Blocked : WireStructuredStatus.Active);
}
