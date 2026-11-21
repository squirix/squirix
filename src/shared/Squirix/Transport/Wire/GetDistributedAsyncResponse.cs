using System;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Transport.Wire.Cache;

internal sealed class GetDistributedAsyncResponse : WireMessage
{
    private ReadOnlyMemory<byte> _shared;
    private byte[]? _owned;

    internal static WireParser<GetDistributedAsyncResponse> Parser { get; } = new(Read);

    internal bool Found { get; set; }

    internal ReadOnlyMemory<byte> Value
    {
        get => _owned is { } owned ? owned : _shared;
        set => SetSharedValue(value);
    }

    internal static GetDistributedAsyncResponse FromOwnedPayload(bool found, byte[] payload) =>
        new() { Found = found, _owned = payload };

    /// <summary>Transfers a newly decoded value to the caller, or copies externally supplied memory.</summary>
    /// <returns>An array owned by the caller.</returns>
    internal byte[] TakeValue()
    {
        if (_owned is not { } owned)
            return Value.ToArray();

        _owned = null;
        return owned;
    }

    internal override void Encode(ref WireWriter writer)
    {
        if (!Found && Value.IsEmpty)
            return;

        writer.Boolean(Found);
        writer.TailBytes(Value);
    }

    private static GetDistributedAsyncResponse Read(ref WireReader reader)
    {
        var response = new GetDistributedAsyncResponse();
        if (reader.Remaining == 0)
            return response;

        response.Found = reader.Boolean();
        var bytes = reader.TailBytes(out response._owned);
        if (response._owned == null)
            response.Value = bytes;

        return response;
    }

    private void SetSharedValue(ReadOnlyMemory<byte> value)
    {
        _owned = null;
        _shared = value;
    }
}
