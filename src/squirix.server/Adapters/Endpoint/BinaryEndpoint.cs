using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authorization.Policy;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Endpoint;

internal static class BinaryEndpoint
{
    private const string ApiPath = "/squirix.cache.v2.SquirixCacheService/";

    private static readonly string[] OperationPaths =
    [
        ApiPath + nameof(BinaryOperation.Unspecified),
        ApiPath + nameof(BinaryOperation.SetEntry),
        ApiPath + nameof(BinaryOperation.GetEntry),
        ApiPath + nameof(BinaryOperation.GetValue),
        ApiPath + nameof(BinaryOperation.Remove),
        ApiPath + nameof(BinaryOperation.TryAddEntry),
        ApiPath + nameof(BinaryOperation.Touch),
        ApiPath + nameof(BinaryOperation.RemoveExpiration),
        ApiPath + nameof(BinaryOperation.Update),
        ApiPath + nameof(BinaryOperation.GetExpiration),
        ApiPath + nameof(BinaryOperation.GetOrAdd),
        ApiPath + nameof(BinaryOperation.SetDistributed),
        ApiPath + nameof(BinaryOperation.GetDistributed),
        ApiPath + nameof(BinaryOperation.RefreshDistributed),
    ];

    internal static ConnectionDelegate Create(ConnectionDelegate next, IServiceProvider services)
    {
        var validator = services.GetService<ConnectionAuthenticationValidator>();
        var authenticate = new ConnectionAuthenticationMiddleware(DispatchAsync, validator, TimeProvider.System);
        return connection => AcceptAsync(connection, next, authenticate, services, validator != null);

        Task DispatchAsync(ConnectionContext connection)
        {
            return ServeAsync(connection, services, true);
        }
    }

    private static async Task AcceptAsync(
        ConnectionContext connection,
        ConnectionDelegate next,
        ConnectionAuthenticationMiddleware authenticate,
        IServiceProvider services,
        bool requiresCredentials)
    {
        var reader = connection.Transport.Input;
        using var timeout = new TimedCancellation(TimeSpan.FromSeconds(5), TimeProvider.System, connection.ConnectionClosed);
        bool hasPreface;
        byte mode;
        try
        {
            (hasPreface, mode) = await ReadPrefaceAsync(reader, timeout.Token).ConfigureAwait(false);
            if (hasPreface)
            {
                var allowed = mode == 1 ? requiresCredentials : mode == 0 && !requiresCredentials;
                var writer = connection.Transport.Output;
                writer.GetSpan(1)[0] = allowed ? Frames.Accepted : Frames.Rejected;
                writer.Advance(1);
                _ = await writer.FlushAsync(timeout.Token).ConfigureAwait(false);
                if (!allowed)
                    return;
            }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            connection.Abort();
            return;
        }

        if (!hasPreface)
            await next(connection).ConfigureAwait(false);
        else if (mode == 1)
            await authenticate.OnConnectionAsync(connection).ConfigureAwait(false);
        else
            await ServeAsync(connection, services, false).ConfigureAwait(false);
    }

    private static async ValueTask<(bool HasPreface, byte Mode)> ReadPrefaceAsync(PipeReader reader, CancellationToken cancellationToken)
    {
        var result = await reader.ReadAtLeastAsync(Frames.PrefaceBytes, cancellationToken).ConfigureAwait(false);
        var buffer = result.Buffer;
        var consumed = buffer.Start;
        try
        {
            if (buffer.Length < Frames.PrefaceBytes)
                return default;

            var mode = ParsePreface(buffer);
            if (mode == null)
                return default;

            consumed = buffer.GetPosition(Frames.PrefaceBytes);
            return (true, mode.Value);
        }
        finally
        {
            reader.AdvanceTo(consumed, consumed);
        }
    }

    private static byte? ParsePreface(ReadOnlySequence<byte> buffer)
    {
        Span<byte> preface = stackalloc byte[Frames.PrefaceBytes];
        buffer.Slice(0, preface.Length).CopyTo(preface);
        return BinaryPrimitives.ReadUInt32BigEndian(preface) == Frames.Preface ? preface[4] : null;
    }

    private static async Task ServeAsync(ConnectionContext connection, IServiceProvider services, bool requiresCredentials)
    {
        var identity = connection.Features.Get<AuthenticatedConnection>();
        if (requiresCredentials && identity == null)
        {
            connection.Abort();
            return;
        }

        var graceful = connection.Features.Get<IConnectionLifetimeNotificationFeature>()?.ConnectionClosedRequested ?? CancellationToken.None;
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(connection.ConnectionClosed, graceful);
        var stop = lifetime.Token;
        var policy = requiresCredentials
            ? await services.GetRequiredService<IAuthorizationPolicyProvider>().GetPolicyAsync(SquirixAuthorizationPolicies.JwtBearer).ConfigureAwait(false) ?? ThrowMissingPolicy()
            : null;
        var accessor = services.GetRequiredService<IHttpContextAccessor>();
        var operations = services.GetRequiredService<BinaryDispatch>();
        var state = new CallState(connection, services, identity, policy, accessor, operations, graceful);
        var log = services.GetRequiredService<ILogger<BinaryConnectionPump>>();
        var pump = new BinaryConnectionPump(connection, (frame, bytes) => ProcessAsync(state, frame, bytes), log, TimeProvider.System);
        await pump.RunAsync(stop).ConfigureAwait(false);
    }

    private static async ValueTask<BinaryResponse> ProcessAsync(CallState state, (int Id, int Size, int Control) frame, ReadOnlyMemory<byte> body)
    {
        var budget = frame.Control & 0xffff;
        if (budget == 0)
            return new BinaryResponse(FrameStatus.DeadlineExceeded);

        var timeout = TimeSpan.FromMilliseconds(Math.Min(budget, RpcAttemptTimeout.OperationDeadline.TotalMilliseconds));
        var operationId = Frames.ReadOperationId(frame.Control);
        var operation = Unsafe.BitCast<int, BinaryOperation>(operationId);
        var deadlineUtc = (TimeProvider.System.GetUtcNow() + timeout).UtcDateTime;
        using var deadline = new TimedCancellation(timeout, TimeProvider.System, state.Connection.ConnectionClosed);
        var scope = state.Services.CreateAsyncScope();
        await using var scopeLifetime = scope.ConfigureAwait(false);
        var http = CreateRequestContext(state, scope.ServiceProvider, operationId, deadline.Token);
        state.Accessor.HttpContext = http;
        try
        {
            var failure = await AuthorizeAsync(state, http, scope.ServiceProvider).ConfigureAwait(false);
            deadline.Token.ThrowIfCancellationRequested();
            if (failure != FrameStatus.Ok)
                return new BinaryResponse(failure);

            string? traceParent = null;
            string? traceState = null;
            if ((frame.Control & Frames.TracedRequest) != 0)
            {
                var traced = BinaryTracedRequest.Parser.ParseFrom(body.Span);
                if (traced.TraceParent.Length > Frames.MaxTraceParentLength || traced.TraceState.Length > Frames.MaxTraceStateLength)
                    return new BinaryResponse(FrameStatus.InvalidArgument);

                traceParent = traced.TraceParent;
                traceState = traced.TraceState.Length == 0 ? null : traced.TraceState;
                body = traced.Payload.Memory;
            }

            var callContext = new BinaryDispatch.Request(http, deadlineUtc, traceParent, traceState);
            return await state.Operations.ExecuteAsync(operation, body, in callContext).ConfigureAwait(false);
        }
        catch (RpcException error)
        {
            return CreateFailure(error);
        }
        catch (InvalidProtocolBufferException)
        {
            return new BinaryResponse(FrameStatus.InvalidArgument);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested && !state.Connection.ConnectionClosed.IsCancellationRequested)
        {
            return new BinaryResponse(FrameStatus.DeadlineExceeded);
        }
        finally
        {
            state.Accessor.HttpContext = null;
        }
    }

    private static DefaultHttpContext CreateRequestContext(CallState state, IServiceProvider services, int operationId, CancellationToken cancellationToken)
    {
        var http = new DefaultHttpContext();
        if (state.Identity != null)
            http.Features.Set(state.Identity);
        http.RequestServices = services;
        http.RequestAborted = cancellationToken;
        http.Request.Scheme = Uri.UriSchemeHttps;
        http.Request.Method = HttpMethods.Post;
        http.Request.Path = operationId < OperationPaths.Length ? OperationPaths[operationId] : OperationPaths[0];
        http.Connection.Id = state.Connection.ConnectionId;
        if (state.Connection.RemoteEndPoint is IPEndPoint remote)
        {
            http.Connection.RemoteIpAddress = remote.Address;
            http.Connection.RemotePort = remote.Port;
        }

        if (state.Connection.LocalEndPoint is not IPEndPoint local)
            return http;

        http.Connection.LocalIpAddress = local.Address;
        http.Connection.LocalPort = local.Port;
        return http;
    }

    private static async ValueTask<FrameStatus> AuthorizeAsync(CallState state, HttpContext http, IServiceProvider services)
    {
        if (state.Policy == null)
            return FrameStatus.Ok;

        var evaluator = services.GetRequiredService<IPolicyEvaluator>();
        var schemes = state.Policy.AuthenticationSchemes;
        var authentication = evaluator.GetType() == typeof(PolicyEvaluator) && schemes.Count == 1
            ? await AuthenticateSingleSchemeAsync(http, schemes[0]).ConfigureAwait(false)
            : await evaluator.AuthenticateAsync(state.Policy, http).ConfigureAwait(false);
        var decision = await evaluator.AuthorizeAsync(state.Policy, authentication, http, http).ConfigureAwait(false);
        if (decision.Succeeded)
            return FrameStatus.Ok;

        var denied = decision.Forbidden ? FrameStatus.PermissionDenied : FrameStatus.Unauthenticated;
        return state.CloseRequested.IsCancellationRequested ? FrameStatus.Unavailable : denied;
    }

    private static async ValueTask<AuthenticateResult> AuthenticateSingleSchemeAsync(HttpContext http, string scheme)
    {
        // The default evaluator only merges principals when more than one scheme can authenticate.
        var authentication = await http.AuthenticateAsync(scheme).ConfigureAwait(false);
        if (authentication is { Succeeded: true })
        {
            http.User = authentication.Principal ?? new ClaimsPrincipal();
            return authentication;
        }

        http.User = new ClaimsPrincipal(new ClaimsIdentity());
        return AuthenticateResult.NoResult();
    }

    private static AuthorizationPolicy ThrowMissingPolicy() => throw new InvalidOperationException("Missing JWT policy for binary transport.");

    private static BinaryResponse CreateFailure(RpcException error)
    {
        var payload = new BinaryCallError { Detail = error.Status.Detail };
        foreach (var trailer in error.Trailers)
        {
            var wire = new BinaryCallError.Types.Trailer { Key = trailer.Key };
            if (trailer.IsBinary)
                wire.Binary = ByteString.CopyFrom(trailer.ValueBytes);
            else
                wire.Text = trailer.Value;
            payload.Trailers.Add(wire);
        }

        return new BinaryResponse(Unsafe.BitCast<StatusCode, FrameStatus>(error.StatusCode), payload);
    }

    private sealed record CallState(
        ConnectionContext Connection,
        IServiceProvider Services,
        AuthenticatedConnection? Identity,
        AuthorizationPolicy? Policy,
        IHttpContextAccessor Accessor,
        BinaryDispatch Operations,
        CancellationToken CloseRequested);
}
