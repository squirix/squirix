namespace Squirix.Server.TestKit.Networking;

/// <summary>Identifies a fixed-size host port region reserved for test infrastructure.</summary>
internal enum HostPortRegion : byte
{
    /// <summary>End-to-end BenchmarkDotNet hosts.</summary>
    EndToEndBenchmarks = 0,

    /// <summary>End-to-end SDK test hosts.</summary>
    EndToEndTests = 1,

    /// <summary>Server smoke test hosts.</summary>
    SmokeTests = 2,

    /// <summary>Server integration test hosts.</summary>
    IntegrationTests = 3,

    /// <summary>In-process server pipeline benchmarks.</summary>
    ServerBenchmarks = 4,

    /// <summary>In-process mock OIDC authority.</summary>
    MockOidcAuthority = 5,

    /// <summary>Cluster mTLS internal listeners.</summary>
    MtlsInternal = 6,

    /// <summary>Server unit test hosts that bind loopback HTTPS listeners.</summary>
    ServerUnitTests = 7,
}
