using Squirix.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Test locator that always returns a fixed owner node id.</summary>
[Immutable]
internal sealed class FixedOwnerLocator : INodeLocator
{
    private readonly string _owner;

    /// <summary>Initializes a new instance of the <see cref="FixedOwnerLocator" /> class.</summary>
    /// <param name="owner">Owner node id returned for every key.</param>
    internal FixedOwnerLocator(string owner)
    {
        _owner = owner;
    }

    /// <inheritdoc />
    public string GetOwner(string cacheName, string key) => _owner;
}
