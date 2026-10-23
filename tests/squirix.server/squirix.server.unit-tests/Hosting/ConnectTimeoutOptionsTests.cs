using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the internode connect timeout of the server options: default, bounds, mapping and settings binding.</summary>
[Immutable]
public sealed class ConnectTimeoutOptionsTests : IsolatedStorageTestBase
{
    private const int DefaultMilliseconds = 500;
    private const long NegativeOffsetTicks = TimeSpan.TicksPerMillisecond * 20;
    private const string SettingsTemplate = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","ForwardConnectTimeout":TIMEOUT}}}""";

    /// <summary>The default connect timeout is valid and equals the maximum, half of the leader refusal margin.</summary>
    [Test]
    public async Task DefaultIsTheMaximum()
    {
        var options = new SquirixServerOptions();

        _ = await Assert.That(options.ForwardConnectTimeout).IsEqualTo(TimeSpan.FromMilliseconds(DefaultMilliseconds));
        _ = await Assert.That(options.ForwardConnectTimeout).IsEqualTo(ForwardingCallPolicyDefaults.MaxConnectTimeout);
        _ = await Assert.That(options.ForwardConnectTimeout).IsEqualTo(ElectionTimerOptions.LeaderRefusalMargin / 2);
        _ = await Assert.That(options.TryValidate(out _)).IsTrue();
    }

    /// <summary>The connect timeout is accepted up to half of the leader refusal margin, and refused above it.</summary>
    /// <param name="ticksFromMaximum">The configured timeout as an offset from the maximum in ticks; zero is the maximum.</param>
    /// <param name="valid">Whether the value is accepted.</param>
    [Test]
    [Arguments(0L, true)]
    [Arguments(-1L, true)]
    [Arguments(1L, false)]
    public async Task ConnectTimeoutUpperBoundIsEnforced(long ticksFromMaximum, bool valid)
    {
        var options = new SquirixServerOptions { ForwardConnectTimeout = ForwardingCallPolicyDefaults.MaxConnectTimeout + TimeSpan.FromTicks(ticksFromMaximum) };

        var result = options.TryValidate(out var errors);

        _ = await Assert.That(result).IsEqualTo(valid);
        if (!valid)
            _ = await Assert.That(errors[0]).Contains("ForwardConnectTimeout", StringComparison.Ordinal);
    }

    /// <summary>The leader refusal margin itself is refused: half of it is kept for the admission gate and the reply.</summary>
    [Test]
    public async Task LeaderRefusalMarginIsRejected()
    {
        var options = new SquirixServerOptions { ForwardConnectTimeout = ElectionTimerOptions.LeaderRefusalMargin };

        _ = await Assert.That(options.TryValidate(out _)).IsFalse();
    }

    /// <summary>The dial minimum is accepted; anything below it, zero and negative values included, is refused.</summary>
    /// <param name="ticksFromMinimum">The configured timeout as an offset from the dial minimum in ticks; zero is the minimum.</param>
    /// <param name="valid">Whether the value is accepted.</param>
    [Test]
    [Arguments(0L, true)]
    [Arguments(-1L, false)]
    [Arguments(-NegativeOffsetTicks, false)]
    public async Task ConnectTimeoutLowerBoundIsEnforced(long ticksFromMinimum, bool valid)
    {
        var options = new SquirixServerOptions { ForwardConnectTimeout = BoundedDial.MinTimeout + TimeSpan.FromTicks(ticksFromMinimum) };

        var result = options.TryValidate(out var errors);

        _ = await Assert.That(result).IsEqualTo(valid);
        if (!valid)
            _ = await Assert.That(errors[0]).Contains("ForwardConnectTimeout", StringComparison.Ordinal);
    }

    /// <summary>The configured timeout reaches the cluster configuration of the host and survives a copy of the options.</summary>
    [Test]
    public async Task TimeoutReachesClusterConfig()
    {
        var source = new SquirixServerOptions { ForwardConnectTimeout = TimeSpan.FromMilliseconds(150) };
        var copy = new SquirixServerOptions();

        Configurator.CopyOptions(source, copy);

        _ = await Assert.That(copy.ForwardConnectTimeout).IsEqualTo(TimeSpan.FromMilliseconds(150));
        _ = await Assert.That(Configurator.ToClusterConfig(source).ForwardConnectTimeout).IsEqualTo(TimeSpan.FromMilliseconds(150));
    }

    /// <summary>A settings file binds the timeout as a TimeSpan string.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileBindsTimeout(CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync("\"00:00:00.150\"", cancellationToken);

        var options = await Configurator.LoadAsync(path, cancellationToken);

        _ = await Assert.That(options.ForwardConnectTimeout).IsEqualTo(TimeSpan.FromMilliseconds(150));
    }

    /// <summary>A settings file whose timeout is above the maximum fails loading and names the setting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsSlowConnect(CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync($"\"{ElectionTimerOptions.LeaderRefusalMargin.ToString("c", CultureInfo.InvariantCulture)}\"", cancellationToken);

        var (success, _, error) = await Configurator.LoadFromFileAsync(path, cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("ForwardConnectTimeout", StringComparison.Ordinal);
    }

    private async Task<string> WriteSettingsAsync(string timeout, CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, SettingsTemplate.Replace("TIMEOUT", timeout, StringComparison.Ordinal), cancellationToken);
        return path;
    }
}
