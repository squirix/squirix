using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint.Resp;

/// <content>Parses arguments for RESP cache writes.</content>
internal sealed partial class CacheCommands
{
    private enum WriteCondition
    {
        /// <summary>Stores regardless of the key's current state.</summary>
        Any = 0,

        /// <summary>Stores only when there is no live key.</summary>
        Absent = 1,

        /// <summary>Stores only when the key is live.</summary>
        Present = 2,
    }

    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct WriteArguments(int ValueIndex, TimeSpan? Expiration, WriteCondition Condition)
    {
        internal static BinaryOperation IdentifySet(RequestReader request)
        {
            if (request.IsCommand(0, "SETNX"u8))
                return BinaryOperation.TryAddEntry;

            for (var index = 3; index < request.Count;)
            {
                if (request.IsCommand(index, "NX"u8))
                    return BinaryOperation.TryAddEntry;
                if (request.IsCommand(index, "XX"u8))
                    return BinaryOperation.Update;
                index += IsTimeOption(request, index) ? 2 : 1;
            }

            return BinaryOperation.SetEntry;
        }

        internal static Response? Parse(RequestReader request, DateTime now, out WriteArguments arguments)
        {
            if (request.IsCommand(0, "SET"u8))
                return ParseSet(request, now, out arguments);
            if (request.IsCommand(0, "SETNX"u8))
            {
                arguments = new WriteArguments(2, null, WriteCondition.Absent);
                return request.Count == 3 ? null : Response.Arity;
            }

            arguments = default;
            if (request.Count != 4)
                return Response.Arity;
            if (!TryReadPositive(request, 2, request.IsCommand(0, "SETEX"u8), now, out var duration))
                return ExpirationArguments.Invalid;

            arguments = new WriteArguments(3, duration, WriteCondition.Any);
            return null;
        }

        private static bool IsTimeOption(RequestReader request, int index) => request.IsCommand(index, "EX"u8) || request.IsCommand(index, "PX"u8);

        private static WriteCondition ReadCondition(RequestReader request, int index)
        {
            if (request.IsCommand(index, "NX"u8))
                return WriteCondition.Absent;
            var present = request.IsCommand(index, "XX"u8);
            return present ? WriteCondition.Present : WriteCondition.Any;
        }

        private static Response? ParseSet(RequestReader request, DateTime now, out WriteArguments arguments)
        {
            arguments = default;
            if (request.Count < 3)
                return Response.Arity;

            TimeSpan? expiry = null;
            var condition = WriteCondition.Any;
            for (var index = 3; index < request.Count;)
            {
                if (IsTimeOption(request, index))
                {
                    if (expiry != null || index + 1 == request.Count)
                        return Response.Syntax;
                    if (!TryReadPositive(request, index + 1, request.IsCommand(index, "EX"u8), now, out var duration))
                        return ExpirationArguments.Invalid;

                    expiry = duration;
                    index += 2;
                    continue;
                }

                if (condition != WriteCondition.Any)
                    return Response.Syntax;
                condition = ReadCondition(request, index);
                if (condition == WriteCondition.Any)
                    return Response.Syntax;
                index++;
            }

            arguments = new WriteArguments(2, expiry, condition);
            return null;
        }

        private static bool TryReadPositive(RequestReader request, int index, bool seconds, DateTime now, out TimeSpan duration) =>
            ExpirationArguments.TryReadDuration(request[index].Span, seconds, now, out duration) && duration > TimeSpan.Zero;
    }
}
