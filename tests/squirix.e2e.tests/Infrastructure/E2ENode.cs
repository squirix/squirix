using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Represents a started test node.
/// </summary>
internal sealed class E2ENode : IAsyncDisposable
{
    private readonly TestNodeHost _host;

    public E2ENode(TestNodeHost host)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
    }

    public string Address => _host.Address;

    public ValueTask DisposeAsync() => _host.DisposeAsync();
}
