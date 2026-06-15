using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Tests strict JSON parsing for durability and mutation payloads.
/// </summary>
public sealed class StrictDurabilityJsonTests : ServerUnitTestBase
{
    /// <summary>
    /// journal envelopes reject duplicate top-level properties instead of choosing one value.
    /// </summary>
    [Fact]
    public void JournalEnvelopeRejectsDuplicateProperties()
    {
        var json = """{"seq":1,"seq":2,"unixMs":1,"remove":{"key":"k"}}"""u8.ToArray();

        _ = Assert.Throws<JsonException>(() => RecordCodec.Deserialize(json));
    }

    /// <summary>
    /// journal parsing rejects malformed envelopes without an operation.
    /// </summary>
    [Fact]
    public void JournalEnvelopeRejectsMissingOperation()
    {
        var json = """{"seq":1,"unixMs":1}"""u8.ToArray();

        var error = Assert.Throws<JsonException>(() => RecordCodec.Deserialize(json));

        Assert.Contains("exactly one operation", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// journal parsing rejects unknown durability fields.
    /// </summary>
    [Fact]
    public void JournalEnvelopeRejectsUnknownCriticalFields()
    {
        var json = """{"seq":1,"unixMs":1,"remove":{"key":"k"},"commitIndex":42}"""u8.ToArray();

        _ = Assert.Throws<JsonException>(() => RecordCodec.Deserialize(json));
    }

    /// <summary>
    /// journal record payloads reject duplicate nested operation properties.
    /// </summary>
    [Fact]
    public void JournalRecordRejectsDuplicateProperties()
    {
        var json = """{"seq":1,"unixMs":1,"remove":{"key":"k1","key":"k2"}}"""u8.ToArray();

        _ = Assert.Throws<JsonException>(() => RecordCodec.Deserialize(json));
    }

    /// <summary>
    /// journal parsing rejects malformed operation payloads.
    /// </summary>
    [Fact]
    public void JournalRecordRejectsMalformedOperation()
    {
        var json = """{"seq":1,"unixMs":1,"put":{"item":{"key":"k"}}}"""u8.ToArray();

        var error = Assert.Throws<JsonException>(() => RecordCodec.Deserialize(json));

        Assert.Contains("entryJsonUtf8", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Manifest files reject duplicate properties instead of choosing one value.
    /// </summary>
    [Fact]
    public void ManifestRejectsDuplicateProperties()
    {
        using var dir = new TempDirectory("squirix-manifest-strict");
        WriteManifestFiles(dir, """{"format":1,"currentJournal":1,"currentJournal":2,"nextSequence":3}""");
        var store = new ManifestStore(new PersistenceOptions { DataDir = dir });

        _ = Assert.Throws<JsonException>(store.ReadCurrentOrDefault);
    }

    /// <summary>
    /// Manifest files reject unknown durability fields.
    /// </summary>
    [Fact]
    public void ManifestRejectsUnknownCriticalFields()
    {
        using var dir = new TempDirectory("squirix-manifest-unknown");
        WriteManifestFiles(dir, """{"format":1,"currentJournal":1,"nextSequence":3,"commitIndex":9}""");
        var store = new ManifestStore(new PersistenceOptions { DataDir = dir });

        _ = Assert.Throws<JsonException>(store.ReadCurrentOrDefault);
    }

    /// <summary>
    /// Snapshot metadata rejects duplicate top-level properties.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task SnapshotMetadataRejectsDuplicateProperties()
    {
        using var dir = new TempDirectory("squirix-snapshot-strict");
        var path = await WriteSnapshotFrameAsync(dir, """{"kind":"idempotency","kind":"entry","idempotency":{"operationId":"op","fingerprint":"fp","createdUtc":"2026-05-01T00:00:00Z","outcome":{"kind":"insert"}}}""");

        _ = await Assert.ThrowsAsync<JsonException>(() => SnapshotReader.LoadStrictAsync<object?>(path, cancellationToken: DefaultCancellationToken));
    }

    /// <summary>
    /// Snapshot metadata rejects malformed/truncated payloads.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task SnapshotMetadataRejectsMalformedPayload()
    {
        using var dir = new TempDirectory("squirix-snapshot-strict");
        var path = await WriteSnapshotFrameAsync(dir, """{"kind":"idempotency","idempotency":{"operationId":"op","fingerprint":"fp","createdUtc":"2026-05-01T00:00:00Z","outcome":{"kind":"cas"}}}""");

        var error = await Assert.ThrowsAsync<InvalidDataException>(() => SnapshotReader.LoadStrictAsync<object?>(path, cancellationToken: DefaultCancellationToken));

        Assert.Contains("outcome kind", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Snapshot metadata rejects unknown top-level fields that affect recovery interpretation.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task SnapshotMetadataRejectsUnknownCriticalFields()
    {
        using var dir = new TempDirectory("squirix-snapshot-strict");
        var path = await WriteSnapshotFrameAsync(dir, """{"kind":"idempotency","idempotency":{"operationId":"op","fingerprint":"fp","createdUtc":"2026-05-01T00:00:00Z","outcome":{"kind":"insert"}},"commitIndex":9}""");

        _ = await Assert.ThrowsAsync<JsonException>(() => SnapshotReader.LoadStrictAsync<object?>(path, cancellationToken: DefaultCancellationToken));
    }

    private static void WriteManifestFiles(string dir, string json)
    {
        const string manifestName = $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}";
        File.WriteAllText(PathKit.Combine(dir, manifestName), json);
        File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}current"), manifestName + Environment.NewLine);
    }

    private static async Task<string> WriteSnapshotFrameAsync(TempDirectory dir, string json)
    {
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Snapshot}000001{StorageFileExtensions.Snapshot}");
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        await FrameCodec.WriteFrameAsync(stream, Encoding.UTF8.GetBytes(json), DefaultCancellationToken);
        return path;
    }
}
