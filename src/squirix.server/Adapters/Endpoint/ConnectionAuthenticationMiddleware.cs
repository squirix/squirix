using System;
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Squirix.Transport.Wire;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Accepts a bounded credential exchange after TLS while leaving ordinary operational HTTP unbound.</summary>
internal sealed class ConnectionAuthenticationMiddleware
{
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private readonly ConnectionDelegate _next;
    private readonly ConnectionAuthenticationValidator? _validator;
    private readonly TimeProvider _timeProvider;

    internal ConnectionAuthenticationMiddleware(ConnectionDelegate next, ConnectionAuthenticationValidator? validator, TimeProvider timeProvider)
    {
        _next = next;
        _validator = validator;
        _timeProvider = timeProvider;
    }

    internal async Task OnConnectionAsync(ConnectionContext context)
    {
        AuthenticatedConnection? identity;
        try
        {
            using var timeout = new CancellationTokenSource(HandshakeTimeout, _timeProvider);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, context.ConnectionClosed);
            var (proceed, accepted) = await ReadAuthenticationAsync(context, linked.Token).ConfigureAwait(false);
            if (!proceed)
                return;
            identity = accepted;
        }
        catch (Exception exception) when (exception is IOException or OperationCanceledException or DecoderFallbackException)
        {
            context.Abort();
            return;
        }

        if (identity == null)
        {
            await _next(context).ConfigureAwait(false);
            return;
        }

        await using var expiry = identity.CreateExpiryTimer(_timeProvider).ConfigureAwait(false);
        await _next(context).ConfigureAwait(false);
    }

    private static (bool IsAuthentication, byte Status, int Length) ParseHeader(in ReadOnlySequence<byte> buffer)
    {
        Span<byte> header = stackalloc byte[ConnectionAuthenticationProtocol.HeaderLength];
        buffer.Slice(0, header.Length).CopyTo(header);
        var status = ConnectionAuthenticationProtocol.ReadRequestHeader(header, out var length);
        return (ConnectionAuthenticationProtocol.HasRequestMagic(header), status, length);
    }

    private static async ValueTask ReplyAsync(ConnectionContext context, byte status, CancellationToken cancellationToken)
    {
        var reply = context.Transport.Output.GetMemory(ConnectionAuthenticationProtocol.HeaderLength);
        ConnectionAuthenticationProtocol.WriteAcknowledgement(reply.Span[..ConnectionAuthenticationProtocol.HeaderLength], status);
        context.Transport.Output.Advance(ConnectionAuthenticationProtocol.HeaderLength);
        _ = await context.Transport.Output.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string? DecodeToken(byte[] payload, int length)
    {
        try
        {
            return StrictUtf8.GetString(payload, 0, length);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    private async ValueTask<(bool Continue, AuthenticatedConnection? Identity)> ReadAuthenticationAsync(ConnectionContext context, CancellationToken cancellationToken)
    {
        var input = context.Transport.Input;
        var result = await input.ReadAtLeastAsync(ConnectionAuthenticationProtocol.HeaderLength, cancellationToken).ConfigureAwait(false);
        var buffer = result.Buffer;
        if (buffer.Length < ConnectionAuthenticationProtocol.HeaderLength)
        {
            input.AdvanceTo(buffer.End);
            context.Abort();
            return (false, null);
        }

        var (isAuthentication, status, length) = ParseHeader(in buffer);
        if (!isAuthentication)
        {
            input.AdvanceTo(buffer.Start, buffer.Start);
            return (true, null);
        }

        input.AdvanceTo(buffer.GetPosition(ConnectionAuthenticationProtocol.HeaderLength));
        if (status != ConnectionAuthenticationProtocol.Accepted || _validator == null ||
            context.Features.Get<IConnectionLifetimeNotificationFeature>() == null)
        {
            await ReplyAsync(context, status == ConnectionAuthenticationProtocol.Accepted ? ConnectionAuthenticationProtocol.InvalidCredentials : status, cancellationToken).ConfigureAwait(false);
            return (false, null);
        }

        return await ReadIdentityAsync(context, length, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<(byte Status, AuthenticatedConnection? Identity)> ValidateTokenAsync(string? token, ConnectionContext context, CancellationToken cancellationToken)
    {
        if (token == null)
            return (ConnectionAuthenticationProtocol.InvalidCredentials, null);
        if (!_validator!.TryEnter())
            return (ConnectionAuthenticationProtocol.Busy, null);
        try
        {
            var identity = await _validator.ValidateAsync(token, context, cancellationToken).ConfigureAwait(false);
            return (identity == null ? ConnectionAuthenticationProtocol.InvalidCredentials : ConnectionAuthenticationProtocol.Accepted, identity);
        }
        finally
        {
            _validator.Exit();
        }
    }

    private async ValueTask<(bool Continue, AuthenticatedConnection? Identity)> ReadIdentityAsync(ConnectionContext context, int length, CancellationToken cancellationToken)
    {
        var input = context.Transport.Input;
        var result = await input.ReadAtLeastAsync(length, cancellationToken).ConfigureAwait(false);
        var buffer = result.Buffer;
        if (buffer.Length < length)
        {
            input.AdvanceTo(buffer.End);
            context.Abort();
            return (false, null);
        }

        var payload = ArrayPool<byte>.Shared.Rent(length);
        AuthenticatedConnection? identity;
        byte status;
        try
        {
            buffer.Slice(0, length).CopyTo(payload);
            input.AdvanceTo(buffer.GetPosition(length));
            var token = DecodeToken(payload, length);
            (status, identity) = await ValidateTokenAsync(token, context, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload, true);
        }

        if (identity == null)
        {
            await ReplyAsync(context, status, cancellationToken).ConfigureAwait(false);
            return (false, null);
        }

        context.Features.Set(identity);
        await ReplyAsync(context, ConnectionAuthenticationProtocol.Accepted, cancellationToken).ConfigureAwait(false);
        return (true, identity);
    }
}
