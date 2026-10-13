using System;
using Squirix.Attributes;
using Squirix.TestKit;

namespace Squirix.UnitTests;

/// <summary>Provides a common base for unit tests, offering safe disposal.</summary>
[Immutable]
public abstract class TestBase
{
    static TestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", PathKit.GetProcTempPath());
    }
}
