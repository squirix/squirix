using System;
using System.Threading;

namespace Squirix.Server.Threading;

/// <summary>Helpers for signalling wait handles that a concurrent owner may dispose.</summary>
internal static class EventWaitHandleExtensions
{
    /// <param name="handle">The wait handle to signal.</param>
    extension(EventWaitHandle handle)
    {
        /// <summary>Signals the handle; a disposal that wins the race is ignored because there is nothing left to wake.</summary>
        internal void SetIfNotDisposed()
        {
            try
            {
                _ = handle.Set();
            }
            catch (ObjectDisposedException)
            {
                // Disposed concurrently: nothing left to wake.
            }
        }
    }
}
