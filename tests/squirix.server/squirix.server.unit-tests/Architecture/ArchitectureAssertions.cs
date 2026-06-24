using System;
using System.Collections.Generic;
using Xunit;
using TestResult = NetArchTest.Rules.TestResult;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Shared assertion helpers for NetArchTest <see cref="TestResult" /> values.
/// </summary>
internal static class ArchitectureAssertions
{
    /// <summary>Fails the test with a sorted, newline-separated list of failing type names when the rule is not satisfied.</summary>
    /// <param name="result">The NetArchTest evaluation result.</param>
    public static void AssertArchitecture(TestResult result)
    {
        if (result.IsSuccessful)
        {
            return;
        }

        var names = new List<string>(result.FailingTypeNames);

        names.Sort(StringComparer.Ordinal);
        Assert.Fail(string.Join(Environment.NewLine, names));
    }
}
