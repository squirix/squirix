using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2ETests.Support.Cluster;

/// <summary>Represents a started test node.</summary>
internal sealed class TestNode : IAsyncDisposable
{
    private readonly TestNodeHost _host;

    public TestNode(TestNodeHost host)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
    }

    public Uri Uri => _host.Uri;

    public ValueTask DisposeAsync() => _host.DisposeAsync();
}
