using System;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Provides a common base for server unit tests.</summary>
[Immutable]
public abstract class ServerUnitTestBase
{
    static ServerUnitTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", NodePathKit.GetProcTempPath());
    }
}
