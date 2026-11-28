using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Adapters.Endpoint.Resp;

/// <summary>Routes RESP cache commands and translates their results.</summary>
[Immutable]
internal sealed partial class CacheCommands
{
    internal const string CacheName = "resp";

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private static readonly Response InvalidKey = Response.Encoded([.. "-ERR invalid cache key\r\n"u8]);
    private static readonly Response InvalidArgument = Response.Encoded([.. "-ERR invalid cache command argument\r\n"u8]);
    private static readonly Response PermissionDenied = Response.Encoded([.. "-NOPERM cache operation denied\r\n"u8]);
    private static readonly Response WrongType = Response.Encoded([.. "-WRONGTYPE operation against a key holding the wrong kind of value\r\n"u8]);
    private static readonly Response CommitOutcomeUnknown = Response.Encoded([.. "-ERR commit outcome unknown\r\n"u8]);
    private static readonly Response Unavailable = Response.Encoded([.. "-ERR cache operation unavailable\r\n"u8]);
    private static readonly Response TooLarge = Response.Encoded([.. "-ERR cached value exceeds the RESP value limit\r\n"u8]);

    private readonly BinaryRequestAccessor _accessor;
    private readonly BinaryDispatch _dispatch;
    private readonly TimeProvider _clock;
    private readonly NativeConnectionAuthenticationValidator? _validator;
    private readonly bool _mutationReplay;

    internal CacheCommands(BinaryRequestAccessor accessor, BinaryDispatch dispatch, NativeConnectionAuthenticationValidator? validator, TimeProvider clock, bool mutationReplay)
    {
        _accessor = accessor;
        _dispatch = dispatch;
        _validator = validator;
        _clock = clock;
        _mutationReplay = mutationReplay;
    }

    internal static BinaryOperation Identify(RequestReader request) => request.Count switch
    {
        _ when request.IsCommand(0, "GET"u8) => BinaryOperation.GetValue,
        _ when request.IsCommand(0, "SET"u8) || request.IsCommand(0, "SETNX"u8) => WriteArguments.IdentifySet(request),
        _ when request.IsCommand(0, "SETEX"u8) || request.IsCommand(0, "PSETEX"u8) => BinaryOperation.SetEntry,
        _ when request.IsCommand(0, "DEL"u8) || request.IsCommand(0, "UNLINK"u8) => BinaryOperation.Remove,
        _ when request.IsCommand(0, "EXISTS"u8) || request.IsCommand(0, "TTL"u8) || request.IsCommand(0, "PTTL"u8) => BinaryOperation.GetExpiration,
        _ when IsExpirationChange(request) => request.Count == 3 && ExpirationArguments.IsImmediate(request[2].Span) ? BinaryOperation.Remove : BinaryOperation.Touch,
        _ when request.IsCommand(0, "PERSIST"u8) => BinaryOperation.RemoveExpiration,
        _ => BinaryOperation.Unspecified,
    };

    internal async ValueTask<Response> ExecuteAsync(BinaryOperation operation, RequestReader request, string connectionId, AuthenticatedConnection? identity, CancellationToken cancellationToken)
    {
        if (_validator != null && identity == null)
            return Response.NoAuth;

        var scope = _validator?.CreateRequestScope();
        try
        {
            var call = new BinaryRequestContext
            {
                ConnectionId = connectionId,
                Path = BinaryEndpoint.GetOperationPath(operation),
                RequestAborted = cancellationToken,
                RequestServices = scope?.ServiceProvider,
            };
            _accessor.Current = call;
            var status = await AuthorizeAsync(operation, identity, call).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return status == FrameStatus.Ok ? await DispatchAsync(operation, request, call).ConfigureAwait(false) : FromStatus(status);
        }
        catch (ServerCallException error)
        {
            return FromStatus(error.StatusCode, error.Error.Detail);
        }
        catch (IOException)
        {
            return Unavailable;
        }
        finally
        {
            _accessor.Current = null;
            if (scope is { } requestScope)
                await requestScope.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static Response FromStatus(FrameStatus status, string? detail = null) => status switch
    {
        FrameStatus.Unauthenticated => Response.NoAuth,
        FrameStatus.PermissionDenied => PermissionDenied,
        FrameStatus.InvalidArgument => InvalidArgument,
        FrameStatus.OutOfRange => TooLarge,
        FrameStatus.ResourceExhausted => Response.Busy,
        FrameStatus.Unavailable when ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(detail) => CommitOutcomeUnknown,
        _ => Unavailable,
    };

    private static Response FromBinaryResult(RequestReader request, BinaryResponse result) =>
        result.Status == FrameStatus.Ok ? ValueReply(request, result.Payload) : FromStatus(result.Status, (result.Payload as BinaryCallError)?.Detail);

    private static Response ValueReply(RequestReader request, WireMessage? payload) => payload switch
    {
        GetValueAsyncResponse { Found: false } => default,
        GetValueAsyncResponse { Value: { KindCase: CacheValue.KindOneofCase.BinaryValue } value } => Response.Bulk(value.BinaryValue),
        GetValueAsyncResponse => WrongType,
        GetExpirationAsyncResponse expiration when request.IsCommand(0, "EXISTS"u8) => Response.Integer(expiration.Found ? 1 : 0),
        GetExpirationAsyncResponse expiration => ExpirationArguments.ReadTtl(request, expiration),
        TouchAsyncResponse touch => Response.Integer(touch.Found ? 1 : 0),
        RemoveExpirationAsyncResponse persist => Response.Integer(persist.Found ? 1 : 0),
        RemoveAsyncResponse removed => Response.Integer(removed.Removed ? 1 : 0),
        TryAddAsyncResponse add when request.IsCommand(0, "SETNX"u8) => Response.Integer(add.Added ? 1 : 0),
        TryAddAsyncResponse add => add.Added ? Response.Ok : default,
        UpdateAsyncResponse replacement => replacement.Updated ? Response.Ok : default,
        SetAsyncResponse => Response.Ok,
        _ => Unavailable,
    };

    private static bool HasValidArity(BinaryOperation operation, RequestReader request) => operation switch
    {
        BinaryOperation.SetEntry or BinaryOperation.TryAddEntry or BinaryOperation.Update => request.Count >= 3,
        _ when IsExpirationChange(request) => request.Count == 3,
        _ => request.Count == 2,
    };

    private static bool IsExpirationChange(RequestReader request) => request.IsCommand(0, "EXPIRE"u8) || request.IsCommand(0, "PEXPIRE"u8);

    private static string? ReadKey(ReadOnlySpan<byte> keyBytes)
    {
        if (keyBytes.Length > CacheKeyValidator.MaxLength * 3)
            return null;
        try
        {
            var key = StrictUtf8.GetString(keyBytes);
            return CacheKeyValidator.TryValidate(key, out _) ? key : null;
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    private async ValueTask<FrameStatus> AuthorizeAsync(BinaryOperation operation, AuthenticatedConnection? identity, BinaryRequestContext call)
    {
        if (_validator == null)
            return FrameStatus.Ok;

        var status = await _validator.AuthorizeAsync(identity!, call, call.RequestAborted).ConfigureAwait(false);
        if (status != FrameStatus.Ok || operation != BinaryOperation.Update)
            return status;

        // SET XX replaces expiry and other entry metadata as well as the value.
        var fullEntry = new BinaryRequestContext
        {
            ConnectionId = call.ConnectionId,
            Path = BinaryEndpoint.GetOperationPath(BinaryOperation.SetEntry),
            RequestAborted = call.RequestAborted,
            RequestServices = call.RequestServices,
        };
        _accessor.Current = fullEntry;
        try
        {
            return await _validator.AuthorizeAsync(identity!, fullEntry, call.RequestAborted).ConfigureAwait(false);
        }
        finally
        {
            _accessor.Current = call;
        }
    }

    private async ValueTask<Response> DispatchAsync(BinaryOperation operation, RequestReader request, BinaryRequestContext call)
    {
        if (!HasValidArity(operation, request))
            return Response.Arity;
        var key = ReadKey(request[1].Span);
        if (key == null)
            return InvalidKey;

        var deadline = new ServerDeadlineBudget(TimeProvider.System, RpcAttemptTimeout.OperationDeadline, TimeProvider.System.GetTimestamp());
        var context = new BinaryDispatch.Request(call, deadline, Task.CompletedTask, ReportUnknownMutationOutcome: true);
        if (operation is BinaryOperation.SetEntry or BinaryOperation.TryAddEntry or BinaryOperation.Update)
            return await WriteAsync(request, key, context).ConfigureAwait(false);
        if (IsExpirationChange(request))
            return await ChangeExpirationAsync(request, key, context).ConfigureAwait(false);

        var result = operation switch
        {
            BinaryOperation.GetValue => await _dispatch.DispatchAsync(
                CacheOperationNames.Get,
                new GetValueAsyncRequest { CacheName = CacheName, Key = key },
                static (ops, input, token) => ops.Reads.GetRespValueAsync(input, token),
                context).ConfigureAwait(false),
            BinaryOperation.GetExpiration => await _dispatch.DispatchAsync(
                CacheOperationNames.GetEntry,
                new GetExpirationAsyncRequest { CacheName = CacheName, Key = key },
                static (ops, input, token) => ops.Reads.GetExpirationAsync(input, token),
                context).ConfigureAwait(false),
            BinaryOperation.Remove => await RemoveAsync(key, in context).ConfigureAwait(false),
            BinaryOperation.RemoveExpiration => await _dispatch.DispatchAsync(
                CacheOperationNames.RemoveExpiration,
                new RemoveExpirationAsyncRequest { CacheName = CacheName, Key = key, OperationId = NewOperationId() },
                static (ops, input, token) => ops.Mutations.RemoveExpirationAsync(input, token),
                context).ConfigureAwait(false),
            _ => new BinaryResponse(FrameStatus.Unimplemented),
        };
        return FromBinaryResult(request, result);
    }

    private async ValueTask<Response> ChangeExpirationAsync(RequestReader request, string key, BinaryDispatch.Request context)
    {
        if (!ExpirationArguments.TryReadDuration(request[2].Span, request.IsCommand(0, "EXPIRE"u8), _clock.GetUtcNow().UtcDateTime, out var duration))
            return ExpirationArguments.Invalid;

        var result = duration > TimeSpan.Zero
            ? await _dispatch.DispatchAsync(
                CacheOperationNames.Touch,
                new TouchAsyncRequest { CacheName = CacheName, Key = key, OperationId = NewOperationId(), Expiration = duration },
                static (ops, input, token) => ops.Mutations.TouchAsync(input, token),
                context).ConfigureAwait(false)
            : await RemoveAsync(key, in context).ConfigureAwait(false);
        return FromBinaryResult(request, result);
    }

    private ValueTask<BinaryResponse> RemoveAsync(string key, in BinaryDispatch.Request context) => _dispatch.DispatchAsync(
        CacheOperationNames.Remove,
        new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = NewOperationId() },
        static (ops, input, token) => ops.Mutations.RemoveAsync(input, token),
        context);

    private async ValueTask<Response> WriteAsync(RequestReader request, string key, BinaryDispatch.Request context)
    {
        var error = WriteArguments.Parse(request, _clock.GetUtcNow().UtcDateTime, out var args);
        if (error is { } failure)
            return failure;

        var entry = new CacheEntryWire
        {
            Value = CacheValue.FromOwnedBinary(request[args.ValueIndex].ToArray()),
            Expiration = args.Expiration,
        };
        var operationId = NewOperationId();
        var result = args.Condition switch
        {
            WriteCondition.Absent => await _dispatch.DispatchAsync(
                CacheOperationNames.TryAdd,
                new TryAddEntryAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId, Entry = entry },
                static (ops, input, token) => ops.Mutations.AddEntryIfAbsentAsync(input, token),
                context).ConfigureAwait(false),
            WriteCondition.Present => await _dispatch.DispatchAsync(
                CacheOperationNames.Update,
                new SetEntryAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId, Entry = entry },
                static (ops, input, token) => ops.Mutations.ReplaceEntryIfPresentAsync(input, token),
                context).ConfigureAwait(false),
            _ => await _dispatch.DispatchAsync(
                CacheOperationNames.Set,
                new SetEntryAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId, Entry = entry },
                static (ops, input, token) => ops.Mutations.SetEntryAsync(input, token),
                context).ConfigureAwait(false),
        };
        return FromBinaryResult(request, result);
    }

    private string NewOperationId() => _mutationReplay ? RpcOperationIdentity.New() : string.Empty;
}
