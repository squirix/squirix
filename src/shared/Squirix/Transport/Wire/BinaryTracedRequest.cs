using System;
using System.Diagnostics;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Transport.Wire.Cache;

internal sealed class BinaryTracedRequest : WireMessage
{
    private ActivityContext? _parent;
    private string? _parentText;

    internal static WireParser<BinaryTracedRequest> Parser { get; } = new(Read);

    internal string TraceParent
    {
        get => _parentText ??= FormatParent();
        init => _parentText = value;
    }

    internal string TraceState { get; init; } = string.Empty;

    internal WireMessage? PayloadMessage { get; init; }

    internal ReadOnlyMemory<byte> Payload { get; init; }

    internal static BinaryTracedRequest Create(in ActivityContext parent, string state, WireMessage payload) =>
        new() { TraceState = state, PayloadMessage = payload, _parent = parent };

    internal override void Encode(ref WireWriter writer)
    {
        if (_parent is { } context)
        {
            writer.Boolean(true);
            writer.TraceParent(in context);
        }
        else
        {
            Span<byte> bytes = stackalloc byte[WireTraceParent.ByteCount];
            var compact = WireTraceParent.TryWrite(TraceParent, bytes);
            writer.Boolean(compact);
            if (compact)
                writer.Raw(bytes);
            else
                writer.Text(TraceParent);
        }

        writer.Text(TraceState);
        if (PayloadMessage is { } body)
            body.Encode(ref writer);
        else
            writer.TailBytes(Payload);
    }

    private static BinaryTracedRequest Read(ref WireReader reader)
    {
        var parent = reader.Boolean() ? WireTraceParent.Read(reader.ReadBytes(WireTraceParent.ByteCount, out _).Span) : reader.Text();
        return new BinaryTracedRequest { TraceParent = parent, TraceState = reader.Text(), Payload = reader.TailBytes(out _) };
    }

    private string FormatParent()
    {
        if (_parent is not { } context)
            return string.Empty;

        Span<byte> bytes = stackalloc byte[WireTraceParent.ByteCount];
        WireTraceParent.Write(bytes, in context);
        return WireTraceParent.Read(bytes);
    }
}
