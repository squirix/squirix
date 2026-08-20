using System;
using System.Runtime.ExceptionServices;

namespace Squirix.Server.UnitTests;

/// <summary>Extensions for surfacing captured faults in tests.</summary>
internal static class ExceptionExtensions
{
    internal static void ThrowIfFaulted(this Exception? error)
    {
        if (error != null)
            ExceptionDispatchInfo.Capture(error).Throw();
    }
}
