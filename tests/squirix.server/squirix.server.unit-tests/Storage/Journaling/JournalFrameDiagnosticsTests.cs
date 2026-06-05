using System;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Focused tests for shared journal frame diagnostics formatting.
/// </summary>
public sealed class JournalFrameDiagnosticsTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies the shared formatter includes status-specific text and frame offsets.
    /// </summary>
    /// <param name="statusName">The read status name to format.</param>
    /// <param name="expected">The expected diagnostic text.</param>
    [Theory]
    [InlineData(nameof(JournalFrameReadStatus.TruncatedHeader), "truncated journal frame header at offset 128")]
    [InlineData(nameof(JournalFrameReadStatus.TruncatedPayload), "truncated journal frame payload at offset 128")]
    [InlineData(nameof(JournalFrameReadStatus.TruncatedChecksum), "truncated journal frame CRC footer at offset 128")]
    [InlineData(nameof(JournalFrameReadStatus.ChecksumMismatch), "journal frame CRC mismatch at offset 128")]
    [InlineData(nameof(JournalFrameReadStatus.OversizedFrame), "declared journal payload length exceeds supported maximum at offset 128")]
    [InlineData(nameof(JournalFrameReadStatus.EndOfFile), "unexpected journal frame EOF at offset 128")]
    public void DescribeReadFailureFormatsStatusAndOffset(string statusName, string expected)
    {
        var status = Enum.Parse<JournalFrameReadStatus>(statusName);
        var read = new JournalFrameReadResult(status, 128, 128);

        var diagnostic = JournalFrameDiagnostics.DescribeReadFailure(read);

        Assert.Equal(expected, diagnostic);
    }

    /// <summary>
    /// Verifies success cannot be formatted as a read failure.
    /// </summary>
    [Fact]
    public void DescribeReadFailureRejectsSuccess()
    {
        var read = new JournalFrameReadResult(JournalFrameReadStatus.Success, 128, 256);

        _ = Assert.Throws<ArgumentOutOfRangeException>(() => JournalFrameDiagnostics.DescribeReadFailure(read));
    }
}
