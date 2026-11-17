using Squirix.Server.TestKit;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Creates JWT credentials for authenticated E2E scenarios.</summary>
internal static class JwtHelper
{
    internal static TestJwtCredentials CreateSymmetricCredentials() =>
        TestJwtHelper.CreateRandomCredentials("https://e2e.squirix.test", "squirix-e2e");
}
