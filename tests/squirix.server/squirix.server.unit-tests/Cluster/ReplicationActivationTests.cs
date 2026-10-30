using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Activation-gate coverage for RF&gt;1 prerequisites.</summary>
[Immutable]
public sealed class ReplicationActivationTests : ServerUnitTestBase
{
    /// <summary>RF=2 with both prerequisites present activates networking with no failures.</summary>
    [Test]
    public async Task RfTwoActivatesWithPrerequisites()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, true, true);
        _ = await Assert.That(failures).IsEmpty();
    }

    /// <summary>RF=2 without both prerequisites reports ordered configuration failures.</summary>
    [Test]
    public async Task RfTwoRequiresPersistenceAndMtls()
    {
        var missingPersistence = new List<string>();
        ReplicationActivationGuard.CollectFailures(missingPersistence, 2, false, false, true);
        await SequenceAssert.EqualAsync([ReplicationActivationGuard.PersistenceRequired], missingPersistence, StringComparer.Ordinal);

        var missingMtls = new List<string>();
        ReplicationActivationGuard.CollectFailures(missingMtls, 2, true, false, true);
        await SequenceAssert.EqualAsync([ReplicationActivationGuard.MtlsRequired], missingMtls, StringComparer.Ordinal);
    }
}
