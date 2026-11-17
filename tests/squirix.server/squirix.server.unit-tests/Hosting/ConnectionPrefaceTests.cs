using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Transport.Grpc;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises bounded server preface reads against real in-memory pipes.</summary>
[Immutable]
public sealed class ConnectionPrefaceTests
{
    /// <summary>Incomplete framing is aborted at the handshake deadline before HTTP admission.</summary>
    [Test]
    public async Task IncompleteHeaderTimesOut()
    {
        var clock = new FakeTimeProvider();
        await using var connection = new ProbeConnection();
        _ = await connection.Incoming.Writer.WriteAsync(new ReadOnlyMemory<byte>([83, 81, 65, 85]), connection.ConnectionClosed);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            null,
            clock);
        var processing = middleware.OnConnectionAsync(connection);
        clock.Advance(TimeSpan.FromSeconds(5));
        await processing;
        _ = await Assert.That(connection.Aborted).IsTrue();
        _ = await Assert.That(admitted).IsFalse();
    }

    /// <summary>Invalid size and unsupported version produce a strict negative acknowledgement.</summary>
    /// <param name="unsupportedVersion">Whether the peer uses an unsupported version instead of an oversized payload.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InvalidHeaderReceivesAcknowledgement(bool unsupportedVersion)
    {
        await using var connection = new ProbeConnection();
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        if (unsupportedVersion)
            header[4] = 2;
        else
            header[6] = 255;
        _ = await connection.Incoming.Writer.WriteAsync(header, connection.ConnectionClosed);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            null,
            TimeProvider.System);
        await middleware.OnConnectionAsync(connection);
        var status = await ReadStatusAsync(connection);
        _ = await Assert.That(status).IsEqualTo(unsupportedVersion ? ConnectionAuthenticationProtocol.UnsupportedVersion : ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(admitted).IsFalse();
    }

    /// <summary>Malformed UTF-8 is an explicit credential denial rather than an ambiguous transport failure.</summary>
    [Test]
    public async Task InvalidUtf8ReceivesCredentialDenial()
    {
        var services = new ServiceCollection();
        _ = services.AddOptions();
        await using var provider = services.BuildServiceProvider();
        using var validator = new ConnectionAuthenticationValidator(provider.GetRequiredService<IOptionsMonitor<JwtBearerOptions>>(), TimeProvider.System, provider.GetRequiredService<IServiceScopeFactory>());
        await using var connection = new ProbeConnection();
        connection.Features.Set<IConnectionLifetimeNotificationFeature>(new Lifetime());
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        _ = await connection.Incoming.Writer.WriteAsync(header, connection.ConnectionClosed);
        _ = await connection.Incoming.Writer.WriteAsync(new ReadOnlyMemory<byte>([255]), connection.ConnectionClosed);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            validator,
            TimeProvider.System);
        await middleware.OnConnectionAsync(connection);
        _ = await Assert.That(await ReadStatusAsync(connection)).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(connection.Aborted).IsFalse();
    }

    private static async Task<byte> ReadStatusAsync(ProbeConnection connection)
    {
        var result = await connection.Outgoing.Reader.ReadAtLeastAsync(8, connection.ConnectionClosed);
        var buffer = result.Buffer;
        var status = ParseStatus(in buffer);
        connection.Outgoing.Reader.AdvanceTo(result.Buffer.End);
        return status;
    }

    private static byte ParseStatus(in ReadOnlySequence<byte> buffer)
    {
        Span<byte> header = stackalloc byte[8];
        buffer.Slice(0, 8).CopyTo(header);
        return ConnectionAuthenticationProtocol.TryReadAcknowledgement(header, out var status) ? status : byte.MaxValue;
    }

    private sealed class ProbeConnection : DefaultConnectionContext
    {
        internal ProbeConnection()
        {
            Transport = new TransportPipe(Incoming.Reader, Outgoing.Writer);
        }

        internal Pipe Incoming { get; } = new();

        internal Pipe Outgoing { get; } = new();

        internal bool Aborted { get; private set; }

        public override void Abort(ConnectionAbortedException abortReason) => Aborted = true;

        public override async ValueTask DisposeAsync()
        {
            await Incoming.Writer.CompleteAsync();
            await Incoming.Reader.CompleteAsync();
            await Outgoing.Writer.CompleteAsync();
            await Outgoing.Reader.CompleteAsync();
            await base.DisposeAsync();
        }
    }

    private sealed class TransportPipe : IDuplexPipe
    {
        internal TransportPipe(PipeReader input, PipeWriter output)
        {
            Input = input;
            Output = output;
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }
    }

    private sealed class Lifetime : IConnectionLifetimeNotificationFeature
    {
        public CancellationToken ConnectionClosedRequested { get; set; }

        public void RequestClose()
        {
        }
    }
}
