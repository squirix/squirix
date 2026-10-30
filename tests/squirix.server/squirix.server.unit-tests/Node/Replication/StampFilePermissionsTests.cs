using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Tests owner-only permissions of the activated topology stamp file.</summary>
[Immutable]
public sealed class StampFilePermissionsTests : ServerUnitTestBase
{
    /// <summary>The activated topology stamp is created owner-only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StampFileIsOwnerOnly(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var root = new TempDirectory("squirix-private-stamp");
        await new ActivatedTopologyStampStore(root).PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }, cancellationToken);

        _ = await Assert.That(File.GetUnixFileMode(Path.Join(root, ActivatedTopologyStampStore.StampFileName))).IsEqualTo(FileEx.PrivateFileMode);
    }
}
