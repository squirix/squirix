using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Logging;

namespace Squirix.Server.TestKit;

/// <summary>Connects node factories to test hosts that supply scoped probes and override dependencies.</summary>
internal static class TestNodeServices
{
    internal static void AddLifetime(TestServiceCollection services)
    {
        services.AddDefaultSingleton<IServerLoggerFactory>(static _ => new TestHostLoggerFactory());
        services.AddDefaultSingleton(static sp => new ServerLifetime(sp.GetRequiredService<IServerLoggerFactory>().CreateLogger<ServerLifetime>()));
        services.AddDefaultSingleton<INodeLifetime>(static sp => sp.GetRequiredService<ServerLifetime>());
    }

    internal static TestServiceCollection AddPersistenceServices(this TestServiceCollection services, PersistenceOptions options, bool waitForRecovery)
    {
        var node = new NodeServices();
        _ = node.AddPersistenceServices(options, waitForRecovery);
        Install(node, services);
        return services;
    }

    internal static TestServiceCollection AddSquirixFrameworkServices(this TestServiceCollection services)
    {
        var node = new NodeServices();
        _ = node.AddSquirixFrameworkServices();
        Install(node, services);
        return services;
    }

    internal static bool AddSquirixSecurityServices(this TestServiceCollection services, SecurityOptions? options)
    {
        var node = new NodeServices();
        var enabled = node.AddSquirixSecurityServices(options);
        if (enabled)
            Install(node, services);
        return enabled;
    }

    internal static async Task ConfigureBuilderAsync(TestServiceCollection services, TopologyOptions cluster, Action<CompositionArgs> configure, CancellationToken token)
    {
        var node = new NodeServices();
        await ServerHostingComposition.ConfigureBuilderAsync(node, cluster, configure, token).ConfigureAwait(false);
        Install(node, services);
    }

    internal static async Task ConfigureNodeAsync(TestServiceCollection services, SquirixServerOptions options, Action<CompositionArgs>? configure, CancellationToken token)
    {
        var node = new NodeServices();
        await NodeConfiguration.ConfigureNodeAsync(node, options, configure, token).ConfigureAwait(false);
        Install(node, services);
    }

    private static void Install(NodeServices node, TestServiceCollection services)
    {
        AddLifetime(services);
        foreach (var registration in node.Registrations)
            services.Add(new TestServiceRegistration(registration.Type, registration.Instance, registration.Factory));
    }
}
