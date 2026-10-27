using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>The windowed snapshot loader fails with the same exception type and message as the record-by-record reader.</summary>
[Immutable]
public sealed class LoadCorruptionTests : IsolatedStorageTestBase
{
    private const int KindOffset = 0;
    private const int LengthOffset = 1;
    private const int PastFooterLength = 1000;
    private const int SmallWindow = 16;
    private const byte UnsupportedKind = 9;

    /// <summary>Flipping any single byte fails identically.</summary>
    /// <param name="windowSize">The window size under test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(SmallWindow)]
    [Arguments(524_288)]
    public async Task EveryByteFlipFailsLikeOracle(int windowSize, CancellationToken cancellationToken)
    {
        var bytes = await BuildSmallSnapshotAsync(cancellationToken);
        for (var index = 0; index < bytes.Length; index++)
        {
            var copy = new byte[bytes.Length];
            bytes.CopyTo(copy, 0);
            copy[index] ^= 0xFF;
            await AssertSameOutcomeAsync(copy, windowSize, cancellationToken);
        }
    }

    /// <summary>Truncating the file at every byte position fails identically.</summary>
    /// <param name="windowSize">The window size under test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(SmallWindow)]
    [Arguments(100)]
    [Arguments(524_288)]
    public async Task EveryTruncationFailsLikeOracle(int windowSize, CancellationToken cancellationToken)
    {
        var bytes = await BuildSmallSnapshotAsync(cancellationToken);
        for (var length = 0; length < bytes.Length; length++)
            await AssertSameOutcomeAsync(bytes[..length], windowSize, cancellationToken);
    }

    /// <summary>A wrong file checksum fails identically.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FileChecksumMismatchFailsLikeOracle(CancellationToken cancellationToken)
    {
        var bytes = await BuildSmallSnapshotAsync(cancellationToken);
        bytes[^1] ^= 0xFF;
        var path = await WriteFileAsync(bytes, cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader(SmallWindow).LoadStrictAsync<object?>(s.path, null, s.cancellationToken).AsTask());

        _ = await Assert.That(failure.Message).IsEqualTo("Binary snapshot file CRC mismatch.");
        await AssertSameOutcomeAsync(bytes, SmallWindow, cancellationToken);
    }

    /// <summary>A declared body length above the maximum is rejected before the body is read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedDeclaredLengthIsRejected(CancellationToken cancellationToken)
    {
        var record = EncodeRecord(RecordKind.Entry, [1, 2, 3]);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(LengthOffset), uint.MaxValue);
        var bytes = LoadSupport.AssembleFile(record);
        var path = await WriteFileAsync(bytes, cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader(SmallWindow).LoadStrictAsync<object?>(s.path, null, s.cancellationToken).AsTask());

        _ = await Assert.That(failure.Message).IsEqualTo("Binary snapshot record body exceeds the maximum allowed length.");
        await AssertSameOutcomeAsync(bytes, SmallWindow, cancellationToken);
    }

    /// <summary>A record whose declared length crosses the footer is rejected before the body is read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordPastFooterIsRejected(CancellationToken cancellationToken)
    {
        var record = EncodeRecord(RecordKind.Entry, [1, 2, 3]);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(LengthOffset), PastFooterLength);
        var bytes = LoadSupport.AssembleFile(record);
        var path = await WriteFileAsync(bytes, cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader(SmallWindow).LoadStrictAsync<object?>(s.path, null, s.cancellationToken).AsTask());

        _ = await Assert.That(failure.Message).IsEqualTo("Binary snapshot record extends past the file footer.");
        await AssertSameOutcomeAsync(bytes, SmallWindow, cancellationToken);
    }

    /// <summary>A record with a valid checksum but an unreadable entry body fails identically.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreadableEntryBodyFailsLikeOracle(CancellationToken cancellationToken)
    {
        var bytes = LoadSupport.AssembleFile(EncodeRecord(RecordKind.Entry, [1, 2, 3]));
        var path = await WriteFileAsync(bytes, cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader(SmallWindow).LoadStrictAsync<object?>(s.path, null, s.cancellationToken).AsTask());

        _ = await Assert.That(failure.Message).IsEqualTo("Binary snapshot entry body could not be read.");
        await AssertSameOutcomeAsync(bytes, SmallWindow, cancellationToken);
    }

    /// <summary>A record with an unknown kind fails identically.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnsupportedKindFailsLikeOracle(CancellationToken cancellationToken)
    {
        var record = EncodeRecord(RecordKind.Entry, [1, 2, 3]);
        record[KindOffset] = UnsupportedKind;
        var bytes = LoadSupport.AssembleFile(record);
        var path = await WriteFileAsync(bytes, cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader(SmallWindow).LoadStrictAsync<object?>(s.path, null, s.cancellationToken).AsTask());

        _ = await Assert.That(failure.Message).IsEqualTo("Unsupported binary snapshot record kind: 9.");
        await AssertSameOutcomeAsync(bytes, SmallWindow, cancellationToken);
    }

    private static byte[] EncodeRecord(RecordKind kind, byte[] body)
    {
        var record = new byte[SnapshotCodec.ComputeRecordLength(body.Length)];
        body.CopyTo(record, SnapshotCodec.RecordHeaderSize);
        SnapshotCodec.SealRecord(record, kind);
        return record;
    }

    private async Task AssertSameOutcomeAsync(byte[] bytes, int windowSize, CancellationToken cancellationToken)
    {
        var path = await WriteFileAsync(bytes, cancellationToken);

        var expected = await LoadSupport.DescribeLoadAsync(new LegacyReader(), path, null, cancellationToken);
        var actual = await LoadSupport.DescribeLoadAsync(StoreFactory.CreateReader(windowSize), path, null, cancellationToken);

        _ = await Assert.That(actual).IsEqualTo(expected);
    }

    private Task<byte[]> BuildSmallSnapshotAsync(CancellationToken cancellationToken) => LoadSupport.WriteAsync(Dir, LoadSupport.BuildItems(4, LoadSupport.SmallLargeValueLength), LoadSupport.BuildIdempotency(), cancellationToken);

    private async Task<string> WriteFileAsync(byte[] bytes, CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "corrupt.snap");
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
        return path;
    }
}
