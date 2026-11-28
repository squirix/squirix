using System;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint.Resp;

/// <summary>Runs one RESP connection in request order.</summary>
internal sealed partial class Session
{
    private static readonly Response Pong = Response.Encoded([.. "+PONG\r\n"u8]);
    private static readonly Response ProtocolError = Response.Encoded([.. "-ERR invalid RESP request framing\r\n"u8]);
    private static readonly Response UnsupportedProtocol = Response.Encoded([.. "-NOPROTO unsupported protocol version\r\n"u8]);
    private static readonly Response UnsupportedDatabase = Response.Encoded([.. "-ERR only database 0 is supported\r\n"u8]);
    private static readonly Response AuthDisabled = Response.Encoded([.. "-ERR authentication is not configured\r\n"u8]);
    private static readonly TimeSpan FirstAuthenticationTimeout = TimeSpan.FromSeconds(5);
    private static readonly Response DeadlineExceeded = Response.Encoded([.. "-ERR operation deadline exceeded\r\n"u8]);

    private readonly NativeConnectionAuthenticationValidator? _validator;
    private readonly CacheCommands _cache;
    private readonly TimeProvider _clock;
    private readonly string _connectionId;
    private readonly Metadata _metadata;
    private readonly RequestReader _request = new();
    private AuthenticatedConnection? _identity;
    private ITimer? _expiry;
    private CancellationTokenSource? _stopReading;
    private int _closeRequested;
    private bool _resp3;

    internal Session(NativeConnectionAuthenticationValidator? validator, CacheCommands cache, long id, TimeProvider clock)
    {
        _validator = validator;
        _clock = clock;
        _metadata = new Metadata(id);
        _cache = cache;
        _connectionId = string.Create(CultureInfo.InvariantCulture, $"resp:{id}");
    }

    internal async Task RunAsync(Stream stream, CancellationToken graceful, CancellationToken abort)
    {
        using var stopReading = new CancellationTokenSource();
        using var reading = CancellationTokenSource.CreateLinkedTokenSource(graceful, abort, stopReading.Token);
        using var firstAuthentication = _validator == null ? null : new TimedCancellation(FirstAuthenticationTimeout, _clock, reading.Token);
        _stopReading = stopReading;
        try
        {
            while (Volatile.Read(ref _closeRequested) == 0 && !reading.IsCancellationRequested)
            {
                var authentication = _identity == null ? firstAuthentication : null;
                var response = await ReadAndExecuteAsync(stream, reading, authentication, abort).ConfigureAwait(false);
                if (response == null || authentication?.IsCancellationRequested == true)
                    return;

                if (authentication != null)
                {
                    await WriteAsync(stream, response.Value, authentication.Token).ConfigureAwait(false);
                    if (_identity != null)
                        authentication.CancelAfter(Timeout.InfiniteTimeSpan);
                }
                else
                {
                    await WriteReadyResponsesAsync(stream, response.Value, reading, abort).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            if (_expiry != null)
                await _expiry.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async ValueTask FlushBatchAsync(Stream stream, ReplyBatch batch, CancellationToken cancellationToken)
    {
        using var writing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        writing.CancelAfter(TimeSpan.FromSeconds(5));
        await stream.WriteAsync(batch.WrittenMemory, writing.Token).ConfigureAwait(false);
    }

    private static async ValueTask FlushBatchAndCancelAsync(Stream stream, ReplyBatch batch, TimedCancellation deadline, CancellationToken cancellationToken)
    {
        try
        {
            await FlushBatchAsync(stream, batch, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            try
            {
                await deadline.CancelAsync().ConfigureAwait(false);
            }
            catch (AggregateException error)
            {
                // Keep the write failure as the session's error if a cancellation callback fails.
                _ = error.InnerExceptions;
            }

            throw;
        }
    }

    private async ValueTask WriteAsync(Stream stream, Response response, CancellationToken cancellationToken)
    {
        using var batch = new ReplyBatch();
        response.AppendTo(batch, _resp3);
        await FlushBatchAsync(stream, batch, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask WriteReadyResponsesAsync(Stream stream, Response first, CancellationTokenSource reading, CancellationToken abort)
    {
        using var batch = new ReplyBatch();
        var response = first;
        var count = 0;
        while (true)
        {
            response.AppendTo(batch, _resp3);
            count++;
            if (count == 16 || batch.WrittenCount >= 65536 || Volatile.Read(ref _closeRequested) != 0 || reading.IsCancellationRequested)
                break;

            try
            {
                if (!_request.TryReadBuffered())
                    break;
            }
            catch (InvalidDataException)
            {
                RequestClose();
                ProtocolError.AppendTo(batch, _resp3);
                break;
            }

#pragma warning disable CA2000 // The loop owns the deadline until both the command and flush complete.
            using var deadline = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, TimeProvider.System, abort);
#pragma warning restore CA2000
            var pending = ExecuteWithDeadlineAsync(deadline, abort);
            if (!pending.IsCompletedSuccessfully)
            {
                var command = pending.AsTask();
                await Task.WhenAll(FlushBatchAndCancelAsync(stream, batch, deadline, abort).AsTask(), command).ConfigureAwait(false);
                batch.Clear();
                count = 0;
                response = await command.ConfigureAwait(false);
            }
            else
            {
                response = await pending.ConfigureAwait(false);
            }
        }

        await FlushBatchAsync(stream, batch, abort).ConfigureAwait(false);
    }

    private async ValueTask<Response?> AuthenticateAsync(ReadOnlyMemory<byte> token, CancellationToken cancellationToken)
    {
        if (_validator == null)
            return AuthDisabled;
        if (token.Length is 0 or > ConnectionAuthenticationProtocol.MaximumTokenBytes || !Ascii.IsValid(token.Span))
            return Response.WrongPass;
        if (!_validator.TryEnter())
            return Response.Busy;

        try
        {
            var identity = await _validator.ValidateAsync(Encoding.ASCII.GetString(token.Span), null, RequestClose, cancellationToken).ConfigureAwait(false);
            if (identity == null)
                return Response.WrongPass;
            if (_expiry != null)
            {
                await _expiry.DisposeAsync().ConfigureAwait(false);
                _expiry = null;
            }

            // Disposing a due timer can suppress its callback before it requests closure.
            if (_identity is { } previous && !_validator.IsWithinLifetime(previous))
                RequestClose();
            if (Volatile.Read(ref _closeRequested) != 0)
                return Response.NoAuth;

            cancellationToken.ThrowIfCancellationRequested();
            _identity = identity;
            _expiry = _validator.CreateExpiryTimer(identity);
            return null;
        }
        finally
        {
            _validator.Exit();
        }
    }

    private async ValueTask<Response> ExecuteAsync(CancellationToken cancellationToken)
    {
        if (IsCommand(0, "AUTH"u8))
            return await ExecuteAuthAsync(cancellationToken).ConfigureAwait(false);
        if (IsCommand(0, "HELLO"u8))
            return await ExecuteHelloAsync(cancellationToken).ConfigureAwait(false);

        var operation = CacheCommands.Identify(_request);
        if (operation != BinaryOperation.Unspecified)
            return await _cache.ExecuteAsync(operation, _request, _connectionId, _identity, cancellationToken).ConfigureAwait(false);

        var authenticated = await IsAuthenticatedAsync(cancellationToken).ConfigureAwait(false);
        return authenticated ? ExecuteCommand() : Response.NoAuth;
    }

    private async ValueTask<Response> ExecuteAuthAsync(CancellationToken cancellationToken) => _request.Count switch
    {
        2 => await AuthenticateAsync(_request[1], cancellationToken).ConfigureAwait(false) ?? Response.Ok,
        3 => _request[1].Span.SequenceEqual("default"u8) ? await AuthenticateAsync(_request[2], cancellationToken).ConfigureAwait(false) ?? Response.Ok : Response.WrongPass,
        _ => Response.Arity,
    };

    private Response ExecuteCommand() => _request.Count switch
    {
        1 when IsCommand(0, "PING"u8) => Pong,
        2 when IsCommand(0, "PING"u8) || IsCommand(0, "ECHO"u8) => Response.Bulk(_request[1]),
        _ when IsCommand(0, "PING"u8) || IsCommand(0, "ECHO"u8) => Response.Arity,
        _ when IsCommand(0, "SELECT"u8) => SelectDatabase(),
        _ when IsCommand(0, "INFO"u8) => Metadata.Info(_request),
        _ when IsCommand(0, "CLIENT"u8) => _metadata.Client(_request),
        _ => Response.Unknown,
    };

    private async ValueTask<Response> ExecuteHelloAsync(CancellationToken cancellationToken)
    {
        if (_request.Count == 1)
            return await IsAuthenticatedAsync(cancellationToken).ConfigureAwait(false) ? _metadata.Hello(_resp3) : Response.NoAuth;

        if (!TryGetProtocol(out var resp3))
            return UnsupportedProtocol;
        if (!TryGetHelloOptions(out var hello))
            return Response.Syntax;

        if (hello.HasPassword)
        {
            if (!_request[hello.PasswordIndex - 1].Span.SequenceEqual("default"u8))
                return Response.WrongPass;
            var auth = await AuthenticateAsync(_request[hello.PasswordIndex], cancellationToken).ConfigureAwait(false);
            if (auth is { } failure)
                return failure;
        }
        else if (!await IsAuthenticatedAsync(cancellationToken).ConfigureAwait(false))
        {
            return Response.NoAuth;
        }

        if (hello.HasName)
            _metadata.Name = _request[hello.NameIndex].ToArray();
        _resp3 = resp3;
        return _metadata.Hello(_resp3);
    }

    private ValueTask<bool> IsAuthenticatedAsync(CancellationToken cancellationToken) =>
        _validator != null && _identity != null ? _validator.IsCurrentAsync(_identity, cancellationToken) : ValueTask.FromResult(_validator == null);

    private bool IsCommand(int index, ReadOnlySpan<byte> command) => _request.IsCommand(index, command);

    private async ValueTask<Response?> ReadAndExecuteAsync(Stream stream, CancellationTokenSource reading, CancellationTokenSource? authentication, CancellationToken abort)
    {
        reading.CancelAfter(TimeSpan.FromMinutes(2));
        try
        {
            if (!await _request.ReadAsync(stream, authentication?.Token ?? reading.Token).ConfigureAwait(false))
                return null;
        }
        catch (InvalidDataException)
        {
            RequestClose();
            return ProtocolError;
        }

        reading.CancelAfter(Timeout.InfiniteTimeSpan);
        return await ExecuteWithDeadlineAsync(authentication, abort).ConfigureAwait(false);
    }

    private async ValueTask<Response> ExecuteWithDeadlineAsync(CancellationTokenSource? authentication, CancellationToken abort)
    {
        var commandToken = authentication?.Token ?? abort;
        using var deadline = new TimedCancellation(RpcAttemptTimeout.OperationDeadline, TimeProvider.System, commandToken);
        return await ExecuteWithDeadlineAsync(deadline, commandToken).ConfigureAwait(false);
    }

    private async ValueTask<Response> ExecuteWithDeadlineAsync(TimedCancellation deadline, CancellationToken commandToken)
    {
        try
        {
            return await ExecuteAsync(deadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !commandToken.IsCancellationRequested)
        {
            return DeadlineExceeded;
        }
    }

    private void RequestClose()
    {
        if (Interlocked.Exchange(ref _closeRequested, 1) == 0)
            _stopReading?.CancelAfter(TimeSpan.Zero);
    }

    private Response SelectDatabase() =>
        _request.Count == 2 ? SelectSupportedDatabase() : Response.Arity;

    private Response SelectSupportedDatabase() => _request[1].Span.SequenceEqual("0"u8) ? Response.Ok : UnsupportedDatabase;

    private bool TryGetHelloOptions(out HelloOptions hello)
    {
        var password = 0;
        var name = 0;
        for (var index = 2; index < _request.Count;)
        {
            if (password == 0 && index + 2 < _request.Count && IsCommand(index, "AUTH"u8))
            {
                index += 2;
                password = index;
            }
            else if (name == 0 && index + 1 < _request.Count && IsCommand(index, "SETNAME"u8) && Metadata.IsValidName(_request[index + 1].Span))
            {
                index++;
                name = index;
            }
            else
            {
                hello = default;
                return false;
            }

            index++;
        }

        hello = new HelloOptions(password, name);
        return true;
    }

    private bool TryGetProtocol(out bool resp3)
    {
        resp3 = _request[1].Span.SequenceEqual("3"u8);
        return resp3 || _request[1].Span.SequenceEqual("2"u8);
    }

    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct HelloOptions(int PasswordIndex, int NameIndex)
    {
        internal bool HasPassword => PasswordIndex != 0;

        internal bool HasName => NameIndex != 0;
    }
}
