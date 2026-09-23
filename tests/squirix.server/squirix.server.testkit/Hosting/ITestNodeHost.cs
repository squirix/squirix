using System;
using System.Threading.Tasks;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Lifecycle contract for an in-process Squirix test node.</summary>
/// <remarks>
/// Tests stop nodes through <see cref="ShutdownAsync" /> (graceful) or
/// <see cref="AbruptShutdownAsync" /> (unclean) instead of relying on disposal.
/// Disposal performs a graceful shutdown when the node is still running.
/// </remarks>
public interface ITestNodeHost : IAsyncDisposable
{
    /// <summary>Gets the absolute path to the node's data directory created for the test run.</summary>
    string DataDir { get; }

    /// <summary>Gets a value indicating whether the internode mTLS listener is enabled for this host.</summary>
    bool HasInterNodeMtlsListener { get; }

    /// <summary>Gets a value indicating whether persistence is enabled for the hosted node.</summary>
    bool PersistenceEnabled { get; }

    /// <summary>Gets the root service provider of the hosted application for resolving test dependencies.</summary>
    IServiceProvider Services { get; }

    /// <summary>Gets the HTTP(S) address where the test node is reachable (e.g., <c language="csharp">https://localhost:9443</c>).</summary>
    Uri Uri { get; }

    /// <summary>Simulates an unclean process termination (for example SIGKILL) by disposing the host without graceful shutdown.</summary>
    ValueTask AbruptShutdownAsync();

    /// <summary>Gracefully stops the node and releases its resources. Safe to call multiple times.</summary>
    ValueTask ShutdownAsync();
}
