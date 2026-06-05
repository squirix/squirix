using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Groups tests that mutate process-wide security environment variables.
/// </summary>
[CollectionDefinition("AuthSensitive", DisableParallelization = true)]
public sealed class AuthSensitiveGroup;
