using System;
using System.Collections.Generic;
using System.Globalization;
using Squirix.E2EBenchmarks.Models;

namespace Squirix.E2EBenchmarks.Harness;

/// <summary>
/// Creates deterministic values for benchmark setup and write paths.
/// </summary>
internal static class E2EBenchmarkDataFactory
{
    private static readonly DateTimeOffset BaseInstant = new(2026, 6, 6, 0, 0, 0, TimeSpan.Zero);

    internal static long CreateLong(int index) => index;

    internal static BenchmarkOrder CreateOrder(int index) => new()
    {
        Id = string.Concat("order-", index.ToString("D8", CultureInfo.InvariantCulture)),
        CustomerId = string.Concat("customer-", (index % 128).ToString("D4", CultureInfo.InvariantCulture)),
        CreatedAt = BaseInstant.AddSeconds(index),
        Lines =
        [
            new BenchmarkOrderLine { Sku = "SKU-001", Quantity = 1 + (index % 5), Price = 9.95m },
            new BenchmarkOrderLine { Sku = "SKU-002", Quantity = 2, Price = 19.50m },
        ],
        Tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["source"] = "benchmark",
            ["bucket"] = (index % 16).ToString(CultureInfo.InvariantCulture),
        },
    };

    internal static string CreateSmallString(int index) => string.Concat("value-", index.ToString("D8", CultureInfo.InvariantCulture));

    internal static BenchmarkUserProfile CreateUserProfile(int index) => new(
        index,
        string.Concat("User ", index.ToString("D8", CultureInfo.InvariantCulture)),
        string.Concat("user", index.ToString("D8", CultureInfo.InvariantCulture), "@example.test"),
        new BenchmarkAddress("Seattle", "Pine Street", (98000 + (index % 100)).ToString(CultureInfo.InvariantCulture)),
        ["reader", "writer"],
        BaseInstant.AddMinutes(index),
        index % 17 == 0 ? BenchmarkUserStatus.Blocked : BenchmarkUserStatus.Active);
}
