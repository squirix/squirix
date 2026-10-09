using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
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
    private const string SettingsTemplate = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","ForwardConnectTimeout":TIMEOUT}}}""";

    /// <summary>The default connect timeout is valid and below the per-attempt timeout of a forwarded call.</summary>
    [Test]
    public async Task DefaultIsBelowAttemptTimeout()
    {
        var options = new SquirixServerOptions();

        _ = await Assert.That(options.ForwardConnectTimeout).IsEqualTo(TimeSpan.FromSeconds(1));
        _ = await Assert.That(options.ForwardConnectTimeout).IsLessThan(ForwardingCallPolicyDefaults.TimeoutPerAttempt);
        _ = await Assert.That(options.TryValidate(out _)).IsTrue();
    }

    /// <summary>The connect timeout is accepted from 10 milliseconds to below the per-attempt timeout, and refused otherwise.</summary>
    /// <param name="milliseconds">The configured timeout in milliseconds.</param>
    /// <param name="valid">Whether the value is accepted.</param>
    [Test]
    [Arguments(-1, false)]
    [Arguments(0, false)]
    [Arguments(1, false)]
    [Arguments(9, false)]
    [Arguments(10, true)]
    [Arguments(300, true)]
    [Arguments(2999, true)]
    [Arguments(3000, false)]
    [Arguments(5000, false)]
    public async Task ConnectTimeoutBoundsAreEnforced(int milliseconds, bool valid)
    {
        var options = new SquirixServerOptions { ForwardConnectTimeout = TimeSpan.FromMilliseconds(milliseconds) };

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

    /// <summary>A settings file whose timeout reaches the per-attempt timeout fails loading and names the setting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsSlowConnect(CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync("\"00:00:05\"", cancellationToken);

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
