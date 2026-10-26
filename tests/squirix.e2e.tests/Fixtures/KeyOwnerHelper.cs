using Squirix.Attributes;
using Squirix.Server.TestKit;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Shared key ownership rings for the e2e node topologies.</summary>
[Immutable]
internal static class KeyOwnerHelper
{
    /// <summary>Shared ring for the default two-node topology (<c language="csharp">nodeA</c>, <c language="csharp">nodeB</c>).</summary>
    internal static readonly TestKeyOwnerHelper TwoNode = new(["nodeA", "nodeB"]);

    /// <summary>Shared ring for the three-node topology (<c language="csharp">nodeA</c>, <c language="csharp">nodeB</c>, <c language="csharp">nodeC</c>).</summary>
    internal static readonly TestKeyOwnerHelper ThreeNode = new(["nodeA", "nodeB", "nodeC"]);

    /// <summary>Shared ring for the five-node topology (<c language="csharp">nodeA</c> to <c language="csharp">nodeE</c>).</summary>
    internal static readonly TestKeyOwnerHelper FiveNode = new(["nodeA", "nodeB", "nodeC", "nodeD", "nodeE"]);
}
