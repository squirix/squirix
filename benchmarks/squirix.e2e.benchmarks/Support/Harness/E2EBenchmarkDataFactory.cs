using System;
using System.Collections.Generic;
using Squirix.E2EBenchmarks.Fixtures;
using Squirix.Server.TestKit;

namespace Squirix.E2EBenchmarks.Support.Harness;

/// <summary>Creates deterministic values for benchmark setup and write paths.</summary>
internal static class E2EBenchmarkDataFactory
{
    private static readonly DateTimeOffset BaseInstant = new(2026, 6, 6, 0, 0, 0, TimeSpan.Zero);

    internal static long CreateLong(int index) => index;

    internal static BenchmarkOrder CreateOrder(int index) => new(
        $"order-{NodeInvariantIndexStrings.FormatD8(index)}",
        $"customer-{NodeInvariantIndexStrings.FormatD4(index % 128)}",
        BaseInstant.AddSeconds(index),
        [
            new BenchmarkOrderLine { Sku = "SKU-001", Quantity = 1 + (index % 5), Price = 9.95m },
            new BenchmarkOrderLine { Sku = "SKU-002", Quantity = 2, Price = 19.50m },
        ],
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["source"] = "benchmark",
            ["bucket"] = NodeInvariantIndexStrings.Format(index % 16),
        });

    internal static string CreateSmallString(int index) => $"value-{NodeInvariantIndexStrings.FormatD8(index)}";

    internal static BenchmarkUserProfile CreateUserProfile(int index) => new(
        index,
        $"User {NodeInvariantIndexStrings.FormatD8(index)}",
        $"user{NodeInvariantIndexStrings.FormatD8(index)}@example.test",
        new BenchmarkAddress("Seattle", "Pine Street", NodeInvariantIndexStrings.Format(98000 + (index % 100))),
        ["reader", "writer"],
        BaseInstant.AddMinutes(index),
        index % 17 is 0 ? BenchmarkUserStatus.Blocked : BenchmarkUserStatus.Active);
}
