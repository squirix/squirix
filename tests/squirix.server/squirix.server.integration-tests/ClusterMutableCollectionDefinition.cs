using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// xUnit collection definition that serializes cluster-mutating integration tests.
/// </summary>
[CollectionDefinition(IntegrationTestCollections.ClusterMutable, DisableParallelization = true)]
public sealed class ClusterMutableCollectionDefinition;
