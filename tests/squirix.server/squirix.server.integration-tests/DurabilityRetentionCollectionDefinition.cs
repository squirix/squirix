using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// xUnit collection definition that serializes durability retention integration tests.
/// </summary>
[CollectionDefinition(IntegrationTestCollections.DurabilityRetention, DisableParallelization = true)]
public sealed class DurabilityRetentionCollectionDefinition;
