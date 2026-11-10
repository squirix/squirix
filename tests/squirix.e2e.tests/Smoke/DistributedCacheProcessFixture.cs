using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests.Smoke;

/// <summary>Owns the HTTPS server process shared by the distributed cache smoke tests.</summary>
[UsedImplicitly]
public sealed class DistributedCacheProcessFixture : IAsyncInitializer, IAsyncDisposable
{
    private ServerProcess? _server;

    internal Uri Uri => _server?.Uri ?? ThrowServerNotStarted();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _server?.DisposeAsync() ?? ValueTask.CompletedTask;

    /// <inheritdoc />
    public async Task InitializeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        _server = await ServerProcess.StartAsync(false, timeout.Token);
    }

    internal static ServiceProvider CreateProvider(Uri uri)
    {
        var services = new ServiceCollection();
        _ = services.AddSquirixCache(options => options.ClientOptions.Endpoints.Add(uri));
        _ = services.AddHybridCache();
        return services.BuildServiceProvider();
    }

    private static Uri ThrowServerNotStarted() => throw new InvalidOperationException("The server process has not started.");
}
