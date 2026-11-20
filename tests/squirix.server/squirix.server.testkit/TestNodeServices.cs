using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;

namespace Squirix.Server.TestKit;

/// <summary>Connects node factories to test hosts that supply scoped probes and override dependencies.</summary>
internal static class TestNodeServices
{
    internal static async Task ConfigureBuilderAsync(IServiceCollection services, TopologyOptions cluster, Action<CompositionArgs> configure, CancellationToken token)
    {
        var node = new NodeServices();
        await ServerHostingComposition.ConfigureBuilderAsync(node, cluster, configure, token).ConfigureAwait(false);
        Install(node, services);
    }

    internal static async Task ConfigureNodeAsync(IServiceCollection services, SquirixServerOptions options, Action<CompositionArgs>? configure, CancellationToken token)
    {
        var node = new NodeServices();
        await NodeConfiguration.ConfigureNodeAsync(node, options, configure, token).ConfigureAwait(false);
        Install(node, services);
    }

    internal static bool AddSquirixSecurityServices(this IServiceCollection services, SecurityOptions? options)
    {
        var node = new NodeServices();
        var enabled = node.AddSquirixSecurityServices(options);
        if (enabled)
            Install(node, services);
        return enabled;
    }

    internal static IServiceCollection AddPersistenceServices(this IServiceCollection services, PersistenceOptions options, bool waitForRecovery)
    {
        var node = new NodeServices();
        _ = node.AddPersistenceServices(options, waitForRecovery);
        Install(node, services);
        return services;
    }

    internal static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services)
    {
        var node = new NodeServices();
        _ = node.AddSquirixFrameworkServices();
        Install(node, services);
        return services;
    }

    internal static void AddLifetime(IServiceCollection services)
    {
        services.TryAddSingleton<Squirix.Server.Logging.ILoggerFactory>(static sp => new Logging.TestHostLoggerFactory(sp.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>()));
        services.TryAddSingleton(static sp => new ServerLifetime(sp.GetRequiredService<Squirix.Server.Logging.ILoggerFactory>().CreateLogger<ServerLifetime>()));
        services.TryAddSingleton<INodeLifetime>(static sp => sp.GetRequiredService<ServerLifetime>());
    }

    private static void Install(NodeServices node, IServiceCollection services)
    {
        _ = services.AddOptions();
        AddLifetime(services);
        _ = services.AddSingleton<INodeRequestScopes>(static sp => new RequestScopes(sp));
        foreach (var registration in node.Registrations)
        {
            if (registration.Instance is Func<JwtValidationOptions> configured)
            {
                AddAuthentication(services, configured);
                continue;
            }

            services.Add(registration.Factory is { } factory
                ? ServiceDescriptor.Singleton(registration.Type, factory!)
                : ServiceDescriptor.Singleton(registration.Type, registration.Instance!));
        }
    }

    private static void AddAuthentication(IServiceCollection services, Func<JwtValidationOptions> configured)
    {
        _ = services.Configure<JwtValidationOptions>(options =>
        {
            var source = configured();
            options.Audience = source.Audience;
            options.Issuer = source.Issuer;
            options.AdditionalIssuers = source.AdditionalIssuers;
            options.SigningKey = source.SigningKey;
            options.Metadata = source.Metadata;
            options.RequireHttpsMetadata = source.RequireHttpsMetadata;
        });
        _ = services.AddSingleton<Func<JwtValidationOptions>>(static sp => () => sp.GetRequiredService<IOptionsMonitor<JwtValidationOptions>>().CurrentValue);
    }

    private sealed class RequestScopes : INodeRequestScopes
    {
        private readonly IServiceProvider _services;

        internal RequestScopes(IServiceProvider services)
        {
            _services = services;
        }

        public bool Contains(Type type) => _services.GetRequiredService<IServiceProviderIsService>().IsService(type);

        public NodeRequestScope Open()
        {
            var scope = _services.CreateAsyncScope();
            return new NodeRequestScope(scope.ServiceProvider, scope);
        }
    }
}
