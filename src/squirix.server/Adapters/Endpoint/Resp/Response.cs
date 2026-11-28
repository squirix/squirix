using System;
using System.Buffers;
using Squirix.Server.Attributes;

namespace Squirix.Server.Adapters.Endpoint.Resp;

[Immutable]
internal readonly record struct Response
{
    internal static readonly Response Ok = Encoded([.. "+OK\r\n"u8]);
    internal static readonly Response Arity = Encoded([.. "-ERR wrong number of arguments\r\n"u8]);
    internal static readonly Response Syntax = Encoded([.. "-ERR syntax error\r\n"u8]);
    internal static readonly Response Unknown = Encoded([.. "-ERR unknown command\r\n"u8]);
    internal static readonly Response NoAuth = Encoded([.. "-NOAUTH Authentication required.\r\n"u8]);
    internal static readonly Response WrongPass = Encoded([.. "-WRONGPASS invalid username-password pair\r\n"u8]);
    internal static readonly Response Busy = Encoded([.. "-BUSY server is busy\r\n"u8]);

    private readonly ReadOnlyMemory<byte> _data;
    private readonly long _number;
    private readonly ResponseKind _kind;

    private Response(ResponseKind kind, ReadOnlyMemory<byte> data = default, long number = default)
    {
        _kind = kind;
        _data = data;
        _number = number;
    }

    internal static Response Bulk(ReadOnlyMemory<byte> value) => new(ResponseKind.Bulk, value);

    internal static Response Encoded(byte[] value) => new(ResponseKind.Encoded, value);

    internal static Response Integer(long value) => new(ResponseKind.Integer, number: value);

    internal void AppendTo(IBufferWriter<byte> buffer, bool resp3)
    {
        switch (_kind)
        {
            case ResponseKind.Null:
                buffer.Write(resp3 ? "_\r\n"u8 : "$-1\r\n"u8);
                break;
            case ResponseKind.Bulk:
                Reply.AppendNumber(buffer, "$"u8[0], _data.Length);
                buffer.Write(_data.Span);
                buffer.Write("\r\n"u8);
                break;
            case ResponseKind.Integer:
                Reply.AppendNumber(buffer, ":"u8[0], _number);
                break;
            case ResponseKind.Encoded:
                buffer.Write(_data.Span);
                break;
            default:
                throw new InvalidOperationException("Unknown response kind.");
        }
    }
}
