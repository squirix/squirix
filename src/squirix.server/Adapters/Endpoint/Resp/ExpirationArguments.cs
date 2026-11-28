using System;
using System.Buffers.Text;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Adapters.Endpoint.Resp;

internal static class ExpirationArguments
{
    internal static readonly Response Invalid = Response.Encoded([.. "-ERR invalid expire time\r\n"u8]);

    internal static bool IsImmediate(ReadOnlySpan<byte> bytes) => TryReadInteger(bytes, out var value) && value <= 0;

    internal static Response ReadTtl(RequestReader request, GetExpirationAsyncResponse result)
    {
        if (!result.Found)
            return Response.Integer(-2);
        if (!result.HasExpiration)
            return Response.Integer(-1);

        var milliseconds = result.Remaining.GetValueOrDefault().Ticks / TimeSpan.TicksPerMillisecond;
        return Response.Integer(request.IsCommand(0, "PTTL"u8) ? milliseconds : (milliseconds + 500) / 1000);
    }

    internal static bool TryReadDuration(ReadOnlySpan<byte> bytes, bool seconds, DateTime now, out TimeSpan duration)
    {
        duration = default;
        if (!TryReadInteger(bytes, out var amount))
            return false;

        var ticksPerUnit = seconds ? TimeSpan.TicksPerSecond : TimeSpan.TicksPerMillisecond;
        if (amount > (DateTime.MaxValue.Ticks - now.Ticks) / ticksPerUnit || (seconds && amount < long.MinValue / 1000))
            return false;
        if (amount > 0)
            duration = TimeSpan.FromTicks(amount * ticksPerUnit);

        return true;
    }

    private static bool TryReadInteger(ReadOnlySpan<byte> bytes, out long amount)
    {
        amount = 0;
        if (bytes.IsEmpty || bytes[0] == '+')
            return false;
        var digits = bytes[0] == '-' ? bytes[1..] : bytes;
        if (!digits.IsEmpty && digits[0] == '0')
            return bytes.Length == 1;

        var parsed = Utf8Parser.TryParse(bytes, out amount, out var consumed);
        return parsed && consumed == bytes.Length;
    }
}
