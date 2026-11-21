using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Logging;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks the structured log contract consumed by disk-recovery diagnostics.</summary>
public sealed class ServerLoggerFactoryTests
{
    /// <summary>The selected JSON formatter writes one parseable event with the original exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JsonFormatterPreservesDiskFailureEvent(CancellationToken cancellationToken)
    {
        const string category = "Squirix.Server.Node.Services.ReplicaGroupCommitter";
        using var directory = new TempDirectory("squirix-console");
        var settings = Path.Join(directory, "appsettings.json");
        await File.WriteAllTextAsync(settings, """{"Logging":{"Console":{"FormatterName":"json","FormatterOptions":{"IncludeScopes":true}}}}""", cancellationToken);
        using var configuration = new ServerLoggingSettings(Path.Join(directory, "defaults.json"), directory, "Production");
        var output = new List<string>();
        using var factory = new ServerLoggerFactory(configuration, output.Add);
        var cause = new IOException("No space left on device\n/data/replication/group.log.");

        var logger = factory.CreateLogger(category);
        using var scope = logger.BeginScope<KeyValuePair<string, object?>[]>([new("node_id", "node-a")]);
        ReplicaCommitLog.ReplicaCommitOutcomeUnknown(logger, cause);
        ReplicaCommitLog.ReplicaTailOfOlderTerm(logger, 41, 12);

        _ = await Assert.That(output.Count).IsEqualTo(2);
        using var document = JsonDocument.Parse(output[0]);
        _ = await Assert.That(document.RootElement.GetProperty("EventId").GetInt32()).IsEqualTo(4006);
        _ = await Assert.That(document.RootElement.GetProperty("Category").GetString()).IsEqualTo(category);
        _ = await Assert.That(document.RootElement.GetProperty("Exception").GetString()).IsEqualTo(cause.ToString());
        _ = await Assert.That(document.RootElement.GetProperty("Scopes")[0].GetProperty("node_id").GetString()).IsEqualTo("node-a");
        using var tail = JsonDocument.Parse(output[1]);
        var state = tail.RootElement.GetProperty("State");
        _ = await Assert.That(state.GetProperty("LastIndex").GetUInt64()).IsEqualTo(41UL);
        _ = await Assert.That(state.GetProperty("Term").GetUInt64()).IsEqualTo(12UL);
    }

    /// <summary>An existing category observes replacement of an environment-specific settings file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FileReplacementRefreshesExistingCategory(CancellationToken cancellationToken)
    {
        using var directory = new TempDirectory("squirix-console-reload");
        var target = Path.Join(directory, "appsettings.Test.json");
        await File.WriteAllTextAsync(target, """{"Logging":{"LogLevel":{"Squirix":"Warning"}}}""", cancellationToken);
        using var settings = new ServerLoggingSettings(Path.Join(directory, "defaults.json"), directory, "Test");
        using var factory = new ServerLoggerFactory(settings, static _ => { });
        var logger = factory.CreateLogger("Squirix.Reload");
        _ = await Assert.That(logger.IsEnabled(ServerLogLevel.Debug)).IsFalse();

        var reloaded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        settings.Changed += (_, _) =>
        {
            if (logger.IsEnabled(ServerLogLevel.Debug))
                _ = reloaded.TrySetResult();
        };

        var replacement = Path.Join(directory, "new.json");
        await File.WriteAllTextAsync(replacement, """{"Logging":{"LogLevel":{"Squirix":"Debug"}}}""", cancellationToken);
        File.Move(target, Path.Join(directory, "previous.json"));
        File.Move(replacement, target);
        await reloaded.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(logger.IsEnabled(ServerLogLevel.Debug)).IsTrue();
    }
}
