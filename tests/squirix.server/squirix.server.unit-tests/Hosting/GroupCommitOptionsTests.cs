using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the public journal options of the server options: defaults, mapping, validation and settings binding.</summary>
[Immutable]
public sealed class GroupCommitOptionsTests : IsolatedStorageTestBase
{
    private const string SettingsTemplate = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","PersistenceEnabled":true,"Journal":JOURNAL}}}""";

    /// <summary>Group commit is off by default with the documented batch cap.</summary>
    [Test]
    public async Task DefaultsKeepGroupCommitOff()
    {
        var options = new SquirixServerOptions();

        _ = await Assert.That(options.Journal.GroupCommitMaxWait).IsEqualTo(TimeSpan.Zero);
        _ = await Assert.That(options.Journal.GroupCommitMaxBatch).IsEqualTo(32);
        _ = await Assert.That(options.TryValidate(out _)).IsTrue();
    }

    /// <summary>The settings map onto the persistence options the host registers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsMapToPersistenceOptions(CancellationToken cancellationToken)
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });
        var dataDirectory = Dir;
        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", port));
                options.UsePersistence(dataDirectory);
                options.Journal.GroupCommitMaxWait = TimeSpan.FromMilliseconds(3);
                options.Journal.GroupCommitMaxBatch = 16;
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        await using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();
        _ = await Assert.That(persistence.JournalGroupCommitMaxWait).IsEqualTo(TimeSpan.FromMilliseconds(3));
        _ = await Assert.That(persistence.JournalGroupCommitMaxBatch).IsEqualTo(16);
        _ = await Assert.That(persistence.IsJournalGroupCommitEnabled).IsTrue();
    }

    /// <summary>Invalid journal options fail the public hosting entry point.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidJournalFailsHosting(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });
        var operation = builder.AddSquirixServerAsync(
            static options =>
            {
                options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort()));
                options.Journal.GroupCommitMaxBatch = 0;
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<ArgumentException>(operation);

        _ = await Assert.That(ex.Message).Contains("GroupCommitMaxBatch", StringComparison.Ordinal);
    }

    /// <summary>CopyOptions carries every journal setting into a new object.</summary>
    [Test]
    public async Task CopyOptionsCopiesJournal()
    {
        var source = new SquirixServerOptions
        {
            Journal = new SquirixServerJournalOptions { GroupCommitMaxWait = TimeSpan.FromMilliseconds(4), GroupCommitMaxBatch = 8 },
        };
        var target = new SquirixServerOptions();

        Configurator.CopyOptions(source, target);

        _ = await Assert.That(target.Journal).IsNotSameReferenceAs(source.Journal);
        _ = await Assert.That(target.Journal.GroupCommitMaxWait).IsEqualTo(TimeSpan.FromMilliseconds(4));
        _ = await Assert.That(target.Journal.GroupCommitMaxBatch).IsEqualTo(8);
    }

    /// <summary>CopyOptions rejects a source whose journal section is null.</summary>
    [Test]
    public void CopyOptionsRejectsNullJournal()
    {
        var source = WithNullJournal();
        var target = new SquirixServerOptions();

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws((source, target), static state => Configurator.CopyOptions(state.source, state.target));
    }

    /// <summary>The batch cap is accepted from one to 4096 and refused outside that range.</summary>
    /// <param name="batch">The configured batch cap.</param>
    /// <param name="valid">Whether the value is accepted.</param>
    [Test]
    [Arguments(0, false)]
    [Arguments(1, true)]
    [Arguments(4096, true)]
    [Arguments(4097, false)]
    [Arguments(int.MaxValue, false)]
    public async Task BatchBoundsAreEnforced(int batch, bool valid)
    {
        var options = new SquirixServerOptions { Journal = new SquirixServerJournalOptions { GroupCommitMaxBatch = batch } };

        var result = options.TryValidate(out var errors);

        _ = await Assert.That(result).IsEqualTo(valid);
        if (!valid)
            _ = await Assert.That(errors[0]).Contains("Journal GroupCommitMaxBatch", StringComparison.Ordinal);
    }

    /// <summary>The wait is accepted as zero or from one to 100 whole milliseconds and refused otherwise.</summary>
    /// <param name="ticks">The configured wait in ticks.</param>
    /// <param name="valid">Whether the value is accepted.</param>
    [Test]
    [Arguments(-TimeSpan.TicksPerMillisecond, false)]
    [Arguments(0L, true)]
    [Arguments(TimeSpan.TicksPerMillisecond / 2, false)]
    [Arguments(TimeSpan.TicksPerMillisecond, true)]
    [Arguments(TimeSpan.TicksPerMillisecond * 3 / 2, false)]
    [Arguments(TimeSpan.TicksPerMillisecond * 100, true)]
    [Arguments(TimeSpan.TicksPerMillisecond * 101, false)]
    public async Task WaitBoundsAreEnforced(long ticks, bool valid)
    {
        var options = new SquirixServerOptions
        {
            PersistenceEnabled = true,
            DataDirectory = Dir,
            Journal = new SquirixServerJournalOptions { GroupCommitMaxWait = TimeSpan.FromTicks(ticks) },
        };

        var result = options.TryValidate(out var errors);

        _ = await Assert.That(result).IsEqualTo(valid);
        if (!valid)
            _ = await Assert.That(errors[0]).Contains("Journal GroupCommitMaxWait", StringComparison.Ordinal);
    }

    /// <summary>A null journal section fails validation with a clear error instead of throwing.</summary>
    [Test]
    public async Task NullJournalFailsValidation()
    {
        var options = WithNullJournal();

        var valid = options.TryValidate(out var errors);

        _ = await Assert.That(valid).IsFalse();
        _ = await Assert.That(errors[0]).IsEqualTo("Journal cannot be null.");
    }

    /// <summary>Enabling group commit without persistence is rejected and says to enable persistence.</summary>
    [Test]
    public async Task WaitWithoutPersistenceFailsValidation()
    {
        var options = new SquirixServerOptions { Journal = new SquirixServerJournalOptions { GroupCommitMaxWait = TimeSpan.FromMilliseconds(2) } };

        _ = await Assert.That(options.TryValidate(out var errors)).IsFalse();
        _ = await Assert.That(errors[0]).Contains("PersistenceEnabled", StringComparison.Ordinal);
    }

    /// <summary>Validate throws for an invalid setting.</summary>
    [Test]
    public async Task ValidateThrowsForInvalidBatch()
    {
        var options = new SquirixServerOptions { Journal = new SquirixServerJournalOptions { GroupCommitMaxBatch = -1 } };

        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(options, static value => value.Validate());
        _ = await Assert.That(ex.Message).Contains("GroupCommitMaxBatch", StringComparison.Ordinal);
    }

    /// <summary>A settings file binds the wait as a TimeSpan string and the batch as a number.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileBindsJournal(CancellationToken cancellationToken)
    {
        var options = await LoadAsync("""{"GroupCommitMaxWait":"00:00:00.002","GroupCommitMaxBatch":64}""", cancellationToken);

        _ = await Assert.That(options.Journal.GroupCommitMaxWait).IsEqualTo(TimeSpan.FromMilliseconds(2));
        _ = await Assert.That(options.Journal.GroupCommitMaxBatch).IsEqualTo(64);
    }

    /// <summary>An empty Journal section keeps the defaults.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileKeepsDefaults(CancellationToken cancellationToken)
    {
        var options = await LoadAsync("{}", cancellationToken);

        _ = await Assert.That(options.Journal.GroupCommitMaxWait).IsEqualTo(TimeSpan.Zero);
        _ = await Assert.That(options.Journal.GroupCommitMaxBatch).IsEqualTo(32);
    }

    /// <summary>A numeric wait is rejected and the error names the field.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsNumericWait(CancellationToken cancellationToken)
    {
        var (success, error) = await LoadResultAsync("""{"GroupCommitMaxWait":5}""", cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("GroupCommitMaxWait", StringComparison.Ordinal);
    }

    /// <summary>A misspelled Journal key fails loading and names the key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsUnknownKey(CancellationToken cancellationToken)
    {
        var (success, error) = await LoadResultAsync("""{"GroupCommitMaxBach":8}""", cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("GroupCommitMaxBach", StringComparison.Ordinal);
    }

    /// <summary>A null Journal section fails loading with the validation error.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsNullJournal(CancellationToken cancellationToken)
    {
        var (success, error) = await LoadResultAsync("null", cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).IsEqualTo("Journal cannot be null.");
    }

    /// <summary>An out-of-range value in the settings file fails loading and names the setting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsFileRejectsOutOfRangeWait(CancellationToken cancellationToken)
    {
        var (success, error) = await LoadResultAsync("""{"GroupCommitMaxWait":"00:00:00.500"}""", cancellationToken);

        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("Journal GroupCommitMaxWait", StringComparison.Ordinal);
    }

    /// <summary>Builds options whose journal section is null the way a settings file with <c language="json">"Journal": null</c> does.</summary>
    /// <returns>The deserialized options.</returns>
    private static SquirixServerOptions WithNullJournal() =>
        ThrowHelper.Required(
            JsonSerializer.Deserialize("""{ "Journal": null }""", SquirixServerHostingJsonContext.Default.SquirixServerOptions),
            "The settings JSON did not produce options.");

    private static string Settings(string journal) => SettingsTemplate.Replace("JOURNAL", journal, StringComparison.Ordinal);

    private async Task<SquirixServerOptions> LoadAsync(string journal, CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync(Settings(journal), cancellationToken);
        return await Configurator.LoadAsync(path, cancellationToken);
    }

    private async Task<(bool Success, string? Error)> LoadResultAsync(string journal, CancellationToken cancellationToken)
    {
        var path = await WriteSettingsAsync(Settings(journal), cancellationToken);
        var (success, _, error) = await Configurator.LoadFromFileAsync(path, cancellationToken);
        return (success, error);
    }

    private async Task<string> WriteSettingsAsync(string json, CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        return path;
    }
}
