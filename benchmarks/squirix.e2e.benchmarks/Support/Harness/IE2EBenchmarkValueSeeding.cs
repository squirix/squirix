using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2EBenchmarks.Support.Harness;

/// <summary>Seed helpers for warming a benchmark keyspace.</summary>
internal interface IE2EBenchmarkValueSeeding
{
    Task SeedAsync(string[] keys, CancellationToken cancellationToken);

    Task SeedExpiringAsync(string[] keys, TimeSpan expiration, CancellationToken cancellationToken);
}
