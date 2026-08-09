using System;
using System.Threading;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Throws a caller-supplied exception on the first flush after arming.</summary>
/// <remarks>Shared one-shot fault hook: three test classes previously carried private copies of this shape.</remarks>
public sealed class ArmableFlushFaultHooks : IFollowerLogFaultHooks
{
    private readonly Func<Exception> _factory;
    private volatile bool _armed;
    private int _fired;

    /// <summary>Initializes a new instance of the <see cref="ArmableFlushFaultHooks"/> class.</summary>
    /// <param name="factory">Creates the exception thrown on the armed flush.</param>
    public ArmableFlushFaultHooks(Func<Exception> factory)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    /// <inheritdoc />
    public void OnBeforeMemoryApply()
    {
    }

    /// <inheritdoc />
    public void OnCommitAdvanced()
    {
    }

    /// <inheritdoc />
    public void OnFrameWritten()
    {
    }

    /// <summary>Throws the caller-supplied exception once; subsequent flushes are no-ops.</summary>
    /// <exception cref="InvalidOperationException">The fault factory returned <see langword="null"/>.</exception>
    public void OnFlushed()
    {
        if (!_armed || Interlocked.Exchange(ref _fired, 1) != 0)
            return;

        throw _factory() ?? throw new InvalidOperationException("The fault factory returned null.");
    }

    /// <summary>Arms the one-shot fault for the next flush; re-arming after a fired fault re-enables it.</summary>
    public void Arm()
    {
        _ = Interlocked.Exchange(ref _fired, 0);
        _armed = true;
    }
}
