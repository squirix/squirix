using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Checks that corruption helpers modify one byte without changing file length.</summary>
[Immutable]
public sealed class FollowerLogTestKitTests : IsolatedStorageTestBase
{
    /// <summary>Only the selected byte changes, including offsets clamped to the tail.</summary>
    /// <param name="offset">The requested offset.</param>
    /// <param name="changedIndex">The byte that must change.</param>
    /// <param name="tail">Whether to use the tail helper.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0, 0, false)]
    [Arguments(2, 2, false)]
    [Arguments(4, 4, false)]
    [Arguments(5, 4, false)]
    [Arguments(int.MaxValue, 4, false)]
    [Arguments(0, 4, true)]
    public async Task OnlySelectedByteChanges(int offset, int changedIndex, bool tail, CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "corrupt.bin");
        byte[] expected = [0, 17, 34, 51, 255];
        await File.WriteAllBytesAsync(path, expected, cancellationToken);

        await CorruptAsync(path, offset, tail, cancellationToken);

        expected[changedIndex] ^= 0xFF;
        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Both helpers reject an empty file without changing it.</summary>
    /// <param name="tail">Whether to use the tail helper.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EmptyFileIsRejected(bool tail, CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "empty.bin");
        await File.WriteAllBytesAsync(path, [], cancellationToken);

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(CorruptAsync(path, 0, tail, cancellationToken), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(error.Message).IsEqualTo($"Cannot corrupt an empty file at '{path}'.");
        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(0L);
    }

    /// <summary>A negative offset is rejected and leaves the file unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NegativeOffsetIsRejected(CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "negative.bin");
        byte[] expected = [1, 2, 3];
        await File.WriteAllBytesAsync(path, expected, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException>(FollowerLogTestKit.CorruptByteAsync(path, -1, cancellationToken), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(expected)).IsTrue();
    }

    private static Task CorruptAsync(string path, int offset, bool tail, CancellationToken cancellationToken) => tail
        ? FollowerLogTestKit.CorruptTailAsync(path, cancellationToken)
        : FollowerLogTestKit.CorruptByteAsync(path, offset, cancellationToken);
}
