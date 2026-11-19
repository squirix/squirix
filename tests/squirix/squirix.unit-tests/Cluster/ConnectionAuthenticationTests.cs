using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Wire;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Verifies the transport authentication framing and rejection boundary.</summary>
[Immutable]
public sealed class ConnectionAuthenticationTests
{
    /// <summary>Checks strict version, size and reserved fields before accepting credentials.</summary>
    /// <param name="index">The header byte to corrupt.</param>
    /// <param name="value">The invalid byte.</param>
    /// <param name="expected">The resulting rejection status.</param>
    [Test]
    [Arguments(0, 0, 1)]
    [Arguments(4, 2, 3)]
    [Arguments(5, 1, 1)]
    [Arguments(6, 255, 1)]
    [Arguments(7, 0, 1)]
    public async Task RejectsMalformedRequest(int index, int value, int expected)
    {
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        header[index] = byte.CreateChecked(value);
        var actual = ConnectionAuthenticationProtocol.ReadRequestHeader(header, out _);
        _ = await Assert.That(actual).IsEqualTo(byte.CreateChecked(expected));
    }

    /// <summary>Preserves the exact bounded UTF-8 credential and calls its provider once.</summary>
    [Test]
    public async Task AcceptedTransportUsesOneCredential()
    {
        var calls = 0;
        await using var stream = new ExchangeStream(0);
        var accepted = await ConnectionAuthentication.AuthenticateAsync(
            stream,
            _ =>
            {
                calls++;
                return ValueTask.FromResult("credential");
            },
            CancellationToken.None);
        var length = ReadLength(stream.Written, out var status);
        var credential = ReadCredential(stream.Written, length);
        _ = await Assert.That(ReferenceEquals(accepted, stream)).IsTrue();
        _ = await Assert.That(calls).IsEqualTo(1);
        _ = await Assert.That(status).IsEqualTo(byte.MinValue);
        _ = await Assert.That(length).IsEqualTo(10);
        _ = await Assert.That(credential).IsEqualTo("credential");
    }

    /// <summary>Explicit credential and protocol rejections are terminal and sanitized.</summary>
    /// <param name="status">The explicit acknowledgement status.</param>
    [Test]
    [Arguments(1)]
    [Arguments(3)]
    public async Task ExplicitRejectionIsTerminal(int status)
    {
        await using var stream = new ExchangeStream(byte.CreateChecked(status));
        var exception = await AsyncAssert.ThrowsAsync<ConnectionRejectedException, Stream>(
            ConnectionAuthentication.AuthenticateAsync(stream, static _ => ValueTask.FromResult("secret"), CancellationToken.None),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            CancellationToken.None);
        var wrapped = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "transport", new HttpRequestException("request", exception)));
        var mapped = ConnectionRejectedException.Map(wrapped);
        _ = await Assert.That(mapped!.StatusCode).IsEqualTo(CacheTransportStatusCode.Unauthenticated);
        _ = await Assert.That(mapped.Status.Detail.Contains("secret", StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Capacity rejections remain retryable transport failures.</summary>
    [Test]
    public async Task BusyRemainsTransient()
    {
        await using var stream = new ExchangeStream(2);
        var exception = await AsyncAssert.ThrowsAsync<IOException, Stream>(
            ConnectionAuthentication.AuthenticateAsync(stream, static _ => ValueTask.FromResult("credential"), CancellationToken.None),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            CancellationToken.None);
        _ = await Assert.That(ConnectionRejectedException.Map(exception)).IsNull();
    }

    /// <summary>Invalid acknowledgement fields cannot be mistaken for a credential denial or success.</summary>
    /// <param name="index">The corrupted field offset.</param>
    /// <param name="value">The invalid field value.</param>
    [Test]
    [Arguments(0, 0)]
    [Arguments(4, 2)]
    [Arguments(5, 4)]
    [Arguments(6, 1)]
    [Arguments(7, 1)]
    public async Task RejectsMalformedAcknowledgement(int index, int value)
    {
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteAcknowledgement(header, 0);
        header[index] = byte.CreateChecked(value);
        _ = await Assert.That(ConnectionAuthenticationProtocol.TryReadAcknowledgement(header, out _)).IsFalse();
    }

    /// <summary>Oversized credentials are rejected locally before any transport bytes are written.</summary>
    [Test]
    public async Task OversizedCredentialNeverWrites()
    {
        await using var stream = new ExchangeStream(0);
        _ = await AsyncAssert.ThrowsAsync<ConnectionRejectedException, Stream>(
            ConnectionAuthentication.AuthenticateAsync(stream, static _ => ValueTask.FromResult(new string('x', 8193)), CancellationToken.None),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            CancellationToken.None);
        _ = await Assert.That(stream.Written.Count).IsEqualTo(0);
    }

    /// <summary>A credential at the UTF-8 byte limit preserves its full payload and byte-count header.</summary>
    [Test]
    public async Task MaximumUtf8CredentialIsWritten()
    {
        var token = new string('\u00e9', 4096);
        await using var stream = new ExchangeStream(ConnectionAuthenticationProtocol.Accepted);
        var accepted = await ConnectionAuthentication.AuthenticateAsync(stream, _ => ValueTask.FromResult(token), CancellationToken.None);
        var length = ReadLength(stream.Written, out var status);
        _ = await Assert.That(Encoding.UTF8.GetByteCount(token)).IsEqualTo(ConnectionAuthenticationProtocol.MaximumTokenBytes);
        _ = await Assert.That(ReferenceEquals(accepted, stream)).IsTrue();
        _ = await Assert.That(status).IsEqualTo(ConnectionAuthenticationProtocol.Accepted);
        _ = await Assert.That(length).IsEqualTo(ConnectionAuthenticationProtocol.MaximumTokenBytes);
        _ = await Assert.That(stream.Written.Count).IsEqualTo(ConnectionAuthenticationProtocol.HeaderLength + ConnectionAuthenticationProtocol.MaximumTokenBytes);
        _ = await Assert.That(ReadCredential(stream.Written, length)).IsEqualTo(token);
    }

    /// <summary>A short string exceeding the UTF-8 byte limit is rejected before writing any transport bytes.</summary>
    [Test]
    public async Task MultibyteOversizeNeverWrites()
    {
        var token = new string('\u00e9', 4097);
        await using var stream = new ExchangeStream(ConnectionAuthenticationProtocol.Accepted);
        _ = await Assert.That(token.Length).IsLessThan(ConnectionAuthenticationProtocol.MaximumTokenBytes);
        _ = await Assert.That(Encoding.UTF8.GetByteCount(token)).IsEqualTo(8194);
        var operation = ConnectionAuthentication.AuthenticateAsync(stream, _ => ValueTask.FromResult(token), CancellationToken.None);
        _ = await AsyncAssert.ThrowsAsync<ConnectionRejectedException, Stream>(
            operation,
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            CancellationToken.None);
        _ = await Assert.That(stream.Written.Count).IsEqualTo(0);
    }

    /// <summary>A wrapped explicit handshake denial stops the configured three-attempt RPC policy immediately.</summary>
    [Test]
    public async Task ExplicitDenialDoesNotRetry()
    {
        await using var policy = new CallPolicy(TimeProvider.System);
        var attempts = new StrongBox<int>();
        var exception = await AsyncAssert.ThrowsAsync<SquirixTransportException, int>(
            policy.ExecuteAsync(
                static (state, _) =>
                {
                    state.Value++;
                    return ValueTask.FromException<int>(new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "transport", new HttpRequestException("request", new ConnectionRejectedException()))));
                },
                attempts,
                CancellationToken.None),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            CancellationToken.None);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(CacheTransportStatusCode.Unauthenticated);
        _ = await Assert.That(attempts.Value).IsEqualTo(1);
    }

    private static int ReadLength(List<byte> written, out byte status)
    {
        status = ConnectionAuthenticationProtocol.ReadRequestHeader(CollectionsMarshal.AsSpan(written)[..8], out var length);
        return length;
    }

    private static string ReadCredential(List<byte> written, int length) => Encoding.UTF8.GetString(CollectionsMarshal.AsSpan(written).Slice(8, length));

    private sealed class ExchangeStream : MemoryStream
    {
        internal ExchangeStream(byte status)
            : base(CreateAcknowledgement(status))
        {
        }

        internal List<byte> Written { get; } = [];

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Written.AddRange(buffer.ToArray());
            return ValueTask.CompletedTask;
        }

        private static byte[] CreateAcknowledgement(byte status)
        {
            var acknowledgement = GC.AllocateUninitializedArray<byte>(8);
            ConnectionAuthenticationProtocol.WriteAcknowledgement(acknowledgement, status);
            return acknowledgement;
        }
    }
}
