using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Portable history artifacts that can be checked again without running the cluster.</summary>
internal static class RegisterHistoryFile
{
    /// <summary>Writes a snapshot, including invocation/response timestamps and ambiguous outcomes.</summary>
    /// <param name="path">Destination file.</param>
    /// <param name="history">The recorded history.</param>
    /// <param name="cancellationToken">Cancellation token for the file write.</param>
    /// <returns>The pending write.</returns>
    internal static async Task SaveAsync(string path, RegisterHistory history, CancellationToken cancellationToken)
    {
        var (writes, reads, failedReads, failedCalls) = history.Snapshot();
        var buffer = new ArrayBufferWriter<byte>();
        await using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteNumber("version", 1);
            json.WriteNumber("failedReads", failedReads);
            json.WriteStartArray("failedReadCalls");
            foreach (var read in failedCalls)
            {
                json.WriteStartArray();
                json.WriteStringValue(read.Key);
                json.WriteNumberValue(read.Start);
                json.WriteNumberValue(read.End);
                json.WriteEndArray();
            }

            json.WriteEndArray();
            json.WriteStartArray("writes");
            foreach (var write in writes)
            {
                json.WriteStartArray();
                json.WriteStringValue(write.Key);
                json.WriteNumberValue(write.Value);
                json.WriteNumberValue(write.Start);
                json.WriteNumberValue(write.End);
                json.WriteBooleanValue(write.Acked);
                json.WriteEndArray();
            }

            json.WriteEndArray();
            json.WriteStartArray("reads");
            foreach (var read in reads)
            {
                json.WriteStartArray();
                json.WriteStringValue(read.Key);
                json.WriteNumberValue(read.Start);
                json.WriteNumberValue(read.End);
                json.WriteNumberValue(read.Observed);
                json.WriteEndArray();
            }

            json.WriteEndArray();
            json.WriteEndObject();
        }

        await File.WriteAllBytesAsync(path, buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Rechecks the exact captured history, independently of scheduler timing on a rerun.</summary>
    /// <param name="path">A version-one history file.</param>
    /// <param name="cancellationToken">Cancellation token for reading and searching.</param>
    /// <returns>The complete check's violations.</returns>
    /// <exception cref="InvalidDataException">The artifact version is unsupported.</exception>
    internal static async Task<List<string>> CheckAsync(string path, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(bytes);
        var root = document.RootElement;
        if (root.GetProperty("version").GetInt32() != 1)
            throw new InvalidDataException("Unsupported register history version.");
        var writes = new List<RegisterWrite>();
        var reads = new List<RegisterRead>();
        foreach (var item in root.GetProperty("writes").EnumerateArray())
            writes.Add(new RegisterWrite(item[0].GetString()!, item[1].GetInt64(), item[2].GetInt64(), item[3].GetInt64(), item[4].GetBoolean()));
        foreach (var item in root.GetProperty("reads").EnumerateArray())
            reads.Add(new RegisterRead(item[0].GetString()!, item[1].GetInt64(), item[2].GetInt64(), item[3].GetInt64()));
        return RegisterLinearizability.Check(writes, reads, cancellationToken);
    }
}
