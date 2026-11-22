using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Portable history artifacts that can be checked again without running the cluster.</summary>
internal static class RegisterHistoryFile
{
    private const int WriteBufferSize = 16 * 1024;

    /// <summary>Writes a snapshot, including invocation/response timestamps and ambiguous outcomes.</summary>
    /// <param name="path">Destination file.</param>
    /// <param name="history">The recorded history.</param>
    /// <param name="cancellationToken">Cancellation token for the file write.</param>
    /// <returns>The pending write.</returns>
    internal static async Task SaveAsync(string path, RegisterHistory history, CancellationToken cancellationToken)
    {
        var (writes, reads, failedReads, failedCalls) = history.Snapshot();
        cancellationToken.ThrowIfCancellationRequested();
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.Read, FileOptions.Asynchronous);
        var buffer = new ArrayBufferWriter<byte>();
        var offset = 0L;
        await using var json = new Utf8JsonWriter(buffer);
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
            if (buffer.WrittenCount + json.BytesPending >= WriteBufferSize)
                offset = await FlushAsync(json, buffer, handle, offset, cancellationToken).ConfigureAwait(false);
        }

        json.WriteEndArray();
        json.WriteStartArray("writes");
        foreach (var write in writes)
        {
            Write(json, in write);
            if (buffer.WrittenCount + json.BytesPending >= WriteBufferSize)
                offset = await FlushAsync(json, buffer, handle, offset, cancellationToken).ConfigureAwait(false);
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
            if (buffer.WrittenCount + json.BytesPending >= WriteBufferSize)
                offset = await FlushAsync(json, buffer, handle, offset, cancellationToken).ConfigureAwait(false);
        }

        json.WriteEndArray();
        json.WriteEndObject();
        _ = await FlushAsync(json, buffer, handle, offset, cancellationToken).ConfigureAwait(false);
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

    private static async ValueTask<long> FlushAsync(Utf8JsonWriter json, ArrayBufferWriter<byte> buffer, SafeFileHandle handle, long offset, CancellationToken cancellationToken)
    {
        await json.FlushAsync(cancellationToken).ConfigureAwait(false);
        await RandomAccess.WriteAsync(handle, buffer.WrittenMemory, offset, cancellationToken).ConfigureAwait(false);
        var end = offset + buffer.WrittenCount;
        buffer.ResetWrittenCount();
        return end;
    }

    private static void Write(Utf8JsonWriter json, in RegisterWrite write)
    {
        json.WriteStartArray();
        json.WriteStringValue(write.Key);
        json.WriteNumberValue(write.Value);
        json.WriteNumberValue(write.Start);
        json.WriteNumberValue(write.End);
        json.WriteBooleanValue(write.Acked);
        json.WriteEndArray();
    }
}
