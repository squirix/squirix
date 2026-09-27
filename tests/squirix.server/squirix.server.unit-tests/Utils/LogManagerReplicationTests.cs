using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Replication lifecycle events that operators must see as errors.</summary>
[Immutable]
public sealed class LogManagerReplicationTests
{
    private const int CoordinatorLeakedOnShutdownEventId = 4008;

    /// <summary>A commit coordinator that leaks its gates to a running commit on shutdown is logged once, as an error.</summary>
    [Test]
    public async Task CoordinatorShutdownLeakLogsError()
    {
        var log = new EventRecordingLogger();

        LogManager.ReplicaCoordinatorLeakedOnShutdown(log, TimeSpan.FromMilliseconds(200));

        _ = await Assert.That(log.Count(CoordinatorLeakedOnShutdownEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(CoordinatorLeakedOnShutdownEventId)?.Level).IsEqualTo(LogLevel.Error);
    }
}
