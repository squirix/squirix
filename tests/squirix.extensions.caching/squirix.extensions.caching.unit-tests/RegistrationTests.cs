using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Extensions.Caching.UnitTests;

/// <summary>Verifies the package's public registration and preflight contract.</summary>
public sealed class RegistrationTests
{
    /// <summary>Invalid settings fail when the provider is constructed.</summary>
    /// <param name="endpoint">The endpoint to configure, if any.</param>
    /// <param name="cacheName">The logical cache name to configure.</param>
    [Test]
    [Arguments(null, "valid")]
    [Arguments("http://localhost:7443", "valid")]
    [Arguments("https://localhost:7443", "bad/name")]
    [SuppressMessage("Usage", "MA0045", Justification = "The assertion invokes the public synchronous constructor.")]
    public async Task InvalidConfigurationFailsBeforeIo(string? endpoint, string cacheName)
    {
        var options = new SquirixCacheOptions { CacheName = cacheName };
        if (endpoint != null)
            options.ClientOptions.Endpoints.Add(new Uri(endpoint));

        var failure = ExceptionAssert.For<OptionsValidationException>().Throws(
            options,
            static settings =>
            {
                using IDisposable cache = new SquirixDistributedCache(Options.Create(settings));
            });
        _ = await Assert.That(failure.OptionsType).IsEqualTo(typeof(SquirixCacheOptions));
    }

    /// <summary>Registration composes options, replaces unkeyed providers and keeps keyed providers.</summary>
    [Test]
    public async Task RegistrationSelectsOneLazySingleton()
    {
        var services = new ServiceCollection();
        _ = services.AddSingleton<IDistributedCache>(static _ => throw new InvalidOperationException("The prior provider was resolved."));
        _ = services.AddKeyedSingleton<IDistributedCache>("other", static (_, _) => throw new InvalidOperationException("The keyed provider was resolved."));
        _ = services.AddSquirixCache(static options => options.CacheName = "composed");
        _ = services.AddSquirixCache(static options => options.ClientOptions.Endpoints.Add(new Uri("https://localhost:7443")));
        var keyed = 0;
        var unkeyed = 0;
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType != typeof(IDistributedCache))
                continue;

            if (descriptor.IsKeyedService)
                keyed++;
            else
                unkeyed++;
        }

        _ = await Assert.That(keyed).IsEqualTo(1);
        _ = await Assert.That(unkeyed).IsEqualTo(1);
        await using var provider = services.BuildServiceProvider();
        _ = await Assert.That(provider.GetRequiredService<IOptions<SquirixCacheOptions>>().Value.CacheName).IsEqualTo("composed");
        var cache = provider.GetRequiredService<IDistributedCache>();
        _ = await Assert.That(cache).IsTypeOf<SquirixDistributedCache>();
        _ = await Assert.That(ReferenceEquals(cache, provider.GetRequiredService<IDistributedCache>())).IsTrue();
    }

    /// <summary>Synchronous calls preserve input and disposal errors.</summary>
    [Test]
    [SuppressMessage("Usage", "MA0045", Justification = "This test exercises the synchronous interface methods.")]
    public async Task SyncCallsPreserveUnderlyingErrors()
    {
        var options = new SquirixCacheOptions();
        options.ClientOptions.Endpoints.Add(new Uri("https://localhost:7443"));
        await using var cache = new SquirixDistributedCache(Options.Create(options));
        var invalid = ExceptionAssert.For<ArgumentException>().Throws(cache, static provider => _ = provider.Get(" "));
        _ = await Assert.That(invalid.ParamName).IsEqualTo("key");
        await cache.DisposeAsync();
        _ = ExceptionAssert.For<ObjectDisposedException>().Throws(cache, static provider => provider.Remove("key"));
    }
}
