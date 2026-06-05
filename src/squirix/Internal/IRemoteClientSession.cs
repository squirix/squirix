using System;

namespace Squirix.Internal;

internal interface IRemoteClientSession : IAsyncDisposable
{
    ICache<T> GetCache<T>(string cacheName);
}
