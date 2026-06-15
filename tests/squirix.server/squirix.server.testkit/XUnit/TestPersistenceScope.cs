using System;
using System.Runtime.CompilerServices;
using Xunit;

namespace Squirix.Server.TestKit.XUnit;

/// <summary>
/// Resolves filesystem path segments for test persistence so journal and snapshot directories stay isolated per xUnit test case.
/// </summary>
public static class TestPersistenceScope
{
    /// <summary>
    /// Returns a stable scope name for the current test run.
    /// When an xUnit test case is active, uses that case’s stable unique id so every case
    /// (including <c>IAsyncLifetime.InitializeAsync</c> and shared helpers) gets a distinct directory.
    /// </summary>
    /// <param name="callerMemberName">
    /// Optional hint when no test case is active (e.g. ad-hoc hosts), usually from <see cref="CallerMemberNameAttribute" />.
    /// </param>
    /// <returns>A non-empty string safe to embed in a path segment.</returns>
    public static string ResolvePersistenceScopeSegment(string? callerMemberName)
    {
        var uniqueId = TestContext.Current.Test?.TestCase.UniqueID;
        var resolvePersistenceScopeSegment = !string.IsNullOrEmpty(callerMemberName) ? callerMemberName : Guid.NewGuid().ToString("N");
        return !string.IsNullOrEmpty(uniqueId) ? uniqueId : resolvePersistenceScopeSegment;
    }
}
