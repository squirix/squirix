using System;
using System.Runtime.CompilerServices;
using TUnit.Core;

namespace Squirix.Server.TestKit;

/// <summary>Resolves filesystem path segments for test persistence so journal and snapshot directories stay isolated per TUnit test.</summary>
public static class TestPersistenceScope
{
    /// <summary>
    /// Returns a stable scope name for the current test run.
    /// When a TUnit test is active, uses that test's stable id so every test
    /// (including <c language="csharp">Before</c> hooks and shared helpers) gets a distinct directory.
    /// </summary>
    /// <param name="callerMemberName">Optional hint when no test is active (e.g., ad-hoc hosts), usually from <see cref="CallerMemberNameAttribute" />.</param>
    /// <returns>A non-empty string safe to embed in a path segment.</returns>
    public static string ResolvePersistenceScopeSegment(string? callerMemberName)
    {
        var uniqueId = TestContext.Current?.Id;
        var resolvePersistenceScopeSegment = !string.IsNullOrEmpty(callerMemberName) ? callerMemberName : Guid.NewGuid().ToString("N");
        return !string.IsNullOrEmpty(uniqueId) ? uniqueId : resolvePersistenceScopeSegment;
    }
}
