using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Signalling a wait handle tolerates a concurrent disposal but still signals live handles.</summary>
[Immutable]
public sealed class EventWaitHandleExtensionsTests
{
    /// <summary>A disposed handle is ignored instead of throwing.</summary>
    [Test]
    public async Task SetOnDisposedHandleDoesNotThrow()
    {
        var handle = new AutoResetEvent(false);
        handle.Dispose();

        handle.SetIfNotDisposed();

        _ = await Assert.That(handle.SafeWaitHandle.IsClosed).IsTrue();
    }

    /// <summary>A live handle is signalled.</summary>
    [Test]
    public async Task SetOnLiveHandleSignals()
    {
        using var handle = new AutoResetEvent(false);

        handle.SetIfNotDisposed();

        _ = await Assert.That(handle.WaitOne(0)).IsTrue();
    }
}
