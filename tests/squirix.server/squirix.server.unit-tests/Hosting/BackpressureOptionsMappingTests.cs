using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the public backpressure options: defaults, mapping, validation and settings binding.</summary>
[Immutable]
public sealed class BackpressureOptionsMappingTests : IsolatedStorageTestBase
{
    /// <summary>Defaults of the public options map to the defaults of the internal admission options.</summary>
    [Test]
    public async Task DefaultsMatchAdmissionDefaults()
    {
        var mapped = new SquirixServerOptions().Backpressure.ToAdmissionOptions();

        _ = await Assert.That(mapped).IsEqualTo(new AdmissionOptions());
        _ = await Assert.That(mapped.Enabled).IsTrue();
        _ = await Assert.That(mapped.HasPerClientLimits).IsFalse();
    }

    /// <summary>Every public setting reaches the matching admission option.</summary>
    [Test]
    public async Task MapsEverySetting()
    {
        var options = new SquirixServerBackpressureOptions
        {
            MaxInFlight = 10,
            MaxQueue = 5,
            MaxQueueWait = TimeSpan.FromSeconds(1),
            MaxSlowdownDelay = TimeSpan.FromMilliseconds(5),
            NodeRateLimitBurst = 40,
            NodeRateLimitPerSecond = 20,
            PerClientMaxInFlight = 3,
            PerClientMaxQueue = 2,
            PerClientRateLimitBurst = 8,
            PerClientRateLimitPerSecond = 4,
            RejectThreshold = 9,
            SlowdownThreshold = 7,
        };

        var mapped = options.ToAdmissionOptions();

        _ = await Assert.That(mapped).IsEqualTo(
            new AdmissionOptions
            {
                MaxInFlight = 10,
                MaxQueue = 5,
                MaxQueueWait = TimeSpan.FromSeconds(1),
                MaxSlowdownDelay = TimeSpan.FromMilliseconds(5),
                NodeRateLimitBurst = 40,
                NodeRateLimitPerSecond = 20,
                PerClientMaxInFlight = 3,
                PerClientMaxQueue = 2,
                PerClientRateLimitBurst = 8,
                PerClientRateLimitPerSecond = 4,
                RejectThreshold = 9,
                SlowdownThreshold = 7,
            });
        _ = await Assert.That(mapped.HasPerClientLimits).IsTrue();
    }

    /// <summary>TryValidate reports the admission rule that a backpressure setting breaks.</summary>
    [Test]
    public async Task ValidateReportsInvalidThresholds()
    {
        var options = new SquirixServerOptions
        {
            Backpressure = new SquirixServerBackpressureOptions { MaxInFlight = 8, SlowdownThreshold = 7, RejectThreshold = 6 },
        };

        var valid = options.TryValidate(out var errors);

        _ = await Assert.That(valid).IsFalse();
        _ = await Assert.That(errors[0]).Contains("RejectThreshold", StringComparison.Ordinal);
    }

    /// <summary>A per-client limit above the node cap fails validation.</summary>
    [Test]
    public async Task ValidateReportsClientLimitAboveCap()
    {
        var options = new SquirixServerOptions
        {
            Backpressure = new SquirixServerBackpressureOptions { MaxInFlight = 8, SlowdownThreshold = 8, RejectThreshold = 8, PerClientMaxInFlight = 9 },
        };

        var valid = options.TryValidate(out var errors);

        _ = await Assert.That(valid).IsFalse();
        _ = await Assert.That(errors[0]).Contains("PerClientMaxInFlight", StringComparison.Ordinal);
    }

    /// <summary>A rate without a burst fails validation.</summary>
    [Test]
    public async Task ValidateReportsRateWithoutBurst()
    {
        var options = new SquirixServerOptions { Backpressure = new SquirixServerBackpressureOptions { PerClientRateLimitPerSecond = 5 } };

        var valid = options.TryValidate(out var errors);

        _ = await Assert.That(valid).IsFalse();
        _ = await Assert.That(errors[0]).Contains("PerClientRateLimitBurst", StringComparison.Ordinal);
    }

    /// <summary>Valid per-client and rate limits pass validation.</summary>
    [Test]
    public async Task ValidateAcceptsClientLimits()
    {
        var options = new SquirixServerOptions
        {
            Backpressure = new SquirixServerBackpressureOptions
            {
                PerClientMaxInFlight = 4,
                PerClientRateLimitPerSecond = 5,
                PerClientRateLimitBurst = 10,
                NodeRateLimitPerSecond = 50,
                NodeRateLimitBurst = 50,
            },
        };

        _ = await Assert.That(options.TryValidate(out _)).IsTrue();
    }

    /// <summary>A null backpressure section is rejected instead of dereferenced.</summary>
    [Test]
    public void ValidateRejectsNullBackpressure()
    {
        var options = new SquirixServerOptions { Backpressure = null! };

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(options, static value => value.TryValidate(out _));
    }

    /// <summary>The settings file binds the Backpressure section, including TimeSpan strings, and keeps defaults for absent keys.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileBindsBackpressure(CancellationToken cancellationToken)
    {
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Backpressure":{"PerClientMaxInFlight":4,"PerClientRateLimitPerSecond":5,"PerClientRateLimitBurst":10,"MaxQueueWait":"00:00:01.500"}}}}""";
        var path = NodePathKit.Combine(Dir, "backpressure.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var options = await Configurator.LoadAsync(path, cancellationToken);

        _ = await Assert.That(options.Backpressure.PerClientMaxInFlight).IsEqualTo(4);
        _ = await Assert.That(options.Backpressure.PerClientRateLimitPerSecond).IsEqualTo(5);
        _ = await Assert.That(options.Backpressure.PerClientRateLimitBurst).IsEqualTo(10);
        _ = await Assert.That(options.Backpressure.MaxQueueWait).IsEqualTo(TimeSpan.FromMilliseconds(1500));
        _ = await Assert.That(options.Backpressure.MaxInFlight).IsEqualTo(256);
        _ = await Assert.That(options.Backpressure.NodeRateLimitPerSecond).IsNull();
    }

    /// <summary>An invalid Backpressure section in the settings file fails loading with the admission error.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsInvalidBackpressure(CancellationToken cancellationToken)
    {
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Backpressure":{"NodeRateLimitPerSecond":10,"NodeRateLimitBurst":5}}}}""";
        var path = NodePathKit.Combine(Dir, "invalid-backpressure.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var (success, _, error) = await Configurator.LoadFromFileAsync(path, cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("NodeRateLimitBurst", StringComparison.Ordinal);
    }

    /// <summary>CopyOptions carries the backpressure settings into the target options.</summary>
    [Test]
    public async Task CopyOptionsCopiesBackpressure()
    {
        var source = new SquirixServerOptions { Backpressure = new SquirixServerBackpressureOptions { PerClientMaxInFlight = 6, MaxQueue = 9 } };
        var target = new SquirixServerOptions();

        Configurator.CopyOptions(source, target);

        _ = await Assert.That(target.Backpressure).IsNotSameReferenceAs(source.Backpressure);
        _ = await Assert.That(target.Backpressure.ToAdmissionOptions()).IsEqualTo(source.Backpressure.ToAdmissionOptions());
    }
}
