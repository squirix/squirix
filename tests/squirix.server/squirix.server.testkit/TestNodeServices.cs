using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
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

    private static void Install(NodeServices node, IServiceCollection services)
    {
        _ = services.AddOptions();
        _ = services.AddSingleton<Squirix.Server.Logging.ILoggerFactory>(static sp => new Logging.TestHostLoggerFactory(sp.GetRequiredService<Microsoft.Extensions.Logging.ILoggerFactory>()));
        _ = services.AddSingleton<INodeLifetime>(static sp => new TestLifetime(sp.GetRequiredService<IHostApplicationLifetime>()));
        _ = services.AddSingleton<INodeRequestScopes>(static sp => new RequestScopes(sp));
        foreach (var registration in node.Registrations)
        {
            if (registration.Instance is Func<JwtValidationOptions> configured)
            {
                AddAuthentication(services, configured);
                continue;
            }

            if (registration.Type == typeof(INodeService))
            {
                AddWorker(services, registration);
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

    private static void AddWorker(IServiceCollection services, NodeServices.Registration registration)
    {
        _ = services.AddKeyedSingleton<INodeService>(registration, (sp, _) => registration.Factory!(sp) is INodeService service ? service : throw new InvalidOperationException());
        _ = services.AddSingleton(sp => sp.GetRequiredKeyedService<INodeService>(registration));
        _ = services.AddSingleton<IHostedService>(sp => new WorkerAdapter(sp.GetRequiredKeyedService<INodeService>(registration)));
    }

    private sealed class WorkerAdapter : BackgroundService, IHostedLifecycleService
    {
        private readonly INodeService _service;

        internal WorkerAdapter(INodeService service)
        {
            _service = service;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await _service.StartAsync(cancellationToken).ConfigureAwait(false);
            await base.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await _service.StopAsync(cancellationToken).ConfigureAwait(false);
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StoppedAsync(CancellationToken cancellationToken) => _service is JournalStopService journal ? journal.DrainAsync() : Task.CompletedTask;

        public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override Task ExecuteAsync(CancellationToken stoppingToken) => (_service as NodeWorker)?.ExecuteTask ?? Task.CompletedTask;
    }

    private sealed class TestLifetime : INodeLifetime
    {
        private readonly IHostApplicationLifetime _lifetime;

        internal TestLifetime(IHostApplicationLifetime lifetime)
        {
            _lifetime = lifetime;
        }

        public CancellationToken ApplicationStarted => _lifetime.ApplicationStarted;

        public CancellationToken ApplicationStopping => _lifetime.ApplicationStopping;

        public void StopApplication() => _lifetime.StopApplication();
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
