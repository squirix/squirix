using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises eviction settings through node startup and configuration validation.</summary>
[Immutable]
public sealed class EvictionSettingsTests : IsolatedStorageTestBase
{
    /// <summary>CLI overrides preserve, replace, or disable the key limit from the settings file.</summary>
    [Test]
    public async Task FlagsOverrideSelectedMode()
    {
        var options = new SquirixServerOptions();
        Configurator.ApplyCacheEvictionOverrides(options, null, null);
        _ = await Assert.That(options.Eviction).IsNull();
        Configurator.ApplyCacheEvictionOverrides(options, "keyCount", 4);
        Configurator.ApplyCacheEvictionOverrides(options, null, null);
        _ = await Assert.That(options.Eviction?.Capacity).IsEqualTo(4);
        Configurator.ApplyCacheEvictionOverrides(options, null, 2);
        _ = await Assert.That(options.Eviction?.Capacity).IsEqualTo(2);
        Configurator.ApplyCacheEvictionOverrides(options, "memory", null);
        _ = await Assert.That(options.Eviction!.EvictOnMemoryPressure).IsTrue();
        _ = await Assert.That(options.Eviction?.Capacity).IsNull();
        Configurator.ApplyCacheEvictionOverrides(options, "disabled", null);
        _ = await Assert.That(options.Eviction).IsNull();
    }

    /// <summary>Loads the selected file and starts the host with its configured eviction mode.</summary>
    /// <param name="mode">Configured mode, or empty to omit the section.</param>
    /// <param name="capacity">Expected key limit, or zero when unlimited.</param>
    /// <param name="memory">Whether to reclaim entries under memory pressure.</param>
    /// <param name="cancellationToken">Test cancellation.</param>
    [Test]
    [Arguments("", 0, false)]
    [Arguments("disabled", 0, false)]
    [Arguments("keyCount", 2, false)]
    [Arguments("memory", 0, true)]
    public async Task SelectedFileControlsMode(string mode, int capacity, bool memory, CancellationToken cancellationToken)
    {
        var section = mode switch
        {
            "" => string.Empty,
            "keyCount" => ""","CacheEviction":{"mode":"keyCount","maxKeys":2}""",
            "memory" => ""","CacheEviction":{"mode":"memory"},"MemoryPressure":{"memoryAccountingEnabled":true,"maxEstimatedCacheBytes":123456}""",
            _ => $$""","CacheEviction":{"mode":"{{mode}}"}""",
        };
        var path = PathKit.Combine(Dir, "custom.json");
        await File.WriteAllTextAsync(path, """{"Cluster":{}""" + section + "}", cancellationToken);
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        var services = TestNodeBuilder.CreateServices();
        _ = await services.ConfigureNodeAsync(
            options => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", held.Port)),
            path,
            configureComposition: static args => args.ManagementUri = new Uri("https://127.0.0.1:0"),
            cancellationToken: cancellationToken);
        await using var host = services.BuildNode();
        held.Dispose();
        await host.StartAsync(cancellationToken);
        try
        {
            if (memory)
                _ = await Assert.That(host.Services.GetRequiredService<PressureOptions>().MaxEstimatedCacheBytes).IsEqualTo(123456L);
            var actual = host.Services.GetRequiredService<EvictionOptions>();
            _ = await Assert.That(actual.Capacity.GetValueOrDefault()).IsEqualTo(capacity);
            _ = await Assert.That(actual.EvictOnMemoryPressure).IsEqualTo(memory);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>Rejects memory eviction when the host explicitly disables accounting, even if the environment enables it.</summary>
    /// <param name="cancellationToken">Test cancellation.</param>
    [Test]
    [NotInParallel]
    public async Task MemoryEvictionRequiresAccounting(CancellationToken cancellationToken)
    {
        using var accounting = new TempEnvironmentVariable("SQUIRIX_MEMORY_PRESSURE_ACCOUNTING_ENABLED", "true");
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        var services = TestNodeBuilder.CreateServices();

        var configure = services.ConfigureNodeAsync(
            options =>
            {
                options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", held.Port));
                options.MemoryAccountingEnabled = false;
                Configurator.ApplyCacheEvictionOverrides(options, "memory", null);
            },
            loadDiscoveredSettings: false,
            configureComposition: static args => args.ManagementUri = new Uri("https://127.0.0.1:0"),
            cancellationToken: cancellationToken);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(configure, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.Message).Contains("Memory-pressure eviction requires memory accounting", StringComparison.Ordinal);
    }

    /// <summary>Rejects invalid modes and limits before the host starts.</summary>
    /// <param name="section">Invalid eviction JSON.</param>
    /// <param name="cancellationToken">Test cancellation.</param>
    [Test]
    [Arguments("""{"mode":"typo"}""")]
    [Arguments("""{"mode":"keyCount"}""")]
    [Arguments("""{"mode":"keyCount","maxKeys":0}""")]
    [Arguments("""{"mode":"memory","maxKeys":2}""")]
    [Arguments("""{"mode":"disabled","maxKeys":2}""")]
    public async Task InvalidModeFailsValidation(string section, CancellationToken cancellationToken)
    {
        var path = PathKit.Combine(Dir, "invalid.json");
        await File.WriteAllTextAsync(path, """{"Squirix":{"Cluster":{},"CacheEviction":""" + section + "}}", cancellationToken);
        var (success, error) = await Configurator.ValidateSettingsFileAsync(path, true, cancellationToken);
        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("CacheEviction.", StringComparison.Ordinal);
    }
}
