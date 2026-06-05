using Xunit;

namespace Squirix.Server.SmokeTests.Security;

/// <summary>
/// Ensures API-key-sensitive smoke tests execute serially to avoid environment-variable races.
/// </summary>
[CollectionDefinition(SmokeTestCollections.AuthSensitive, DisableParallelization = true)]
public sealed class AuthSensitiveSmokeTestCollectionDefinition;
