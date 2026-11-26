"""Generate the closed cache and replication codecs from their positional wire schemas."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "src/shared/Squirix/Transport/Wire"
REPLICA = ROOT / "src/squirix.server/Adapters/Wire/Replication"

# Each schema line names a message followed by Type:Property fields in wire order.
# '?' marks presence, '*' a sequence, and '|' a discriminated union member.
# Presence bits and booleans share a leading byte. The union selector follows
# regular fields and is the one-based position within the union (zero is unset).
# A top-level frame can carry its final bytes without a second length prefix.
# BinaryTracedRequest writes a compact W3C parent and streams its nested RPC body.
# ReplicaTransferEntries shares one cache name across a homogeneous batch.
# GetDistributedAsyncResponse owns decoded reply bytes. Its handwritten codec lives beside the cache models.
# AppendReplicaEntriesRequest implements both explicit and sequential batches beside the replication models.
CACHE = """
CacheEntryWire ?CacheValue:Value ?Timestamp:ExpiresUtc ?Duration:Expiration
CacheValue |Null:NullValue |String:StringValue |Int64:Int64Value |Double:DoubleValue |Bool:BoolValue |Bytes:JsonUtf8 |Bytes:BinaryValue
SetEntryAsyncRequest String:CacheName String:Key ?CacheEntryWire:Entry ?OperationId:OperationId
SetAsyncResponse
GetEntryAsyncRequest String:CacheName String:Key
GetEntryAsyncResponse Bool:Found ?CacheEntryWire:Entry ?Duration:Remaining
GetValueAsyncRequest String:CacheName String:Key
RemoveAsyncRequest String:CacheName String:Key ?OperationId:OperationId
RemoveAsyncResponse Bool:Removed
TryAddEntryAsyncRequest String:CacheName String:Key ?CacheEntryWire:Entry ?OperationId:OperationId
TryAddAsyncResponse Bool:Added
TouchAsyncRequest String:CacheName String:Key ?Duration:Expiration ?OperationId:OperationId
TouchAsyncResponse Bool:Found
RemoveExpirationAsyncRequest String:CacheName String:Key ?OperationId:OperationId
RemoveExpirationAsyncResponse Bool:Found
UpdateAsyncRequest String:CacheName String:Key ?CacheEntryWire:Entry ?OperationId:OperationId
UpdateAsyncResponse Bool:Updated
GetExpirationAsyncRequest String:CacheName String:Key
GetExpirationAsyncResponse Bool:Found Bool:HasExpiration ?Duration:Remaining
GetOrAddAsyncRequest String:CacheName String:Key ?CacheEntryWire:Entry ?OperationId:OperationId
GetOrAddAsyncResponse Bool:Added ?CacheValue:Value Bool:Found
SetDistributedAsyncRequest String:CacheName String:Key ?Int64:ExpiresAtUnixMs ?Int64:SlidingTicks ?OperationId:OperationId ?Int64:TtlTicks TailBytes:Value
GetDistributedAsyncRequest String:CacheName String:Key
BinaryCallError String:Detail *Trailer:Trailers
RefreshDistributedAsyncRequest String:CacheName String:Key ?OperationId:OperationId
RefreshDistributedAsyncResponse
Trailer String:Key |String:Text |Bytes:Binary
"""
REPLICATION = """
AppendReplicaEntriesResponse UInt64:Term UInt64:LastLogIndex Bool:Success String:RefusalCode Bool:MemoryPressure
GetReplicaStatusRequest ?EnvelopeHeader:Header
GetReplicaStatusResponse UInt64:Term String:Role UInt64:LastLogIndex UInt64:CommitIndex String:Readiness Bytes:TopologyFingerprint UInt64:ConfigurationGeneration String:RefusalCode UInt64:LastLogTerm UInt64:AppliedIndex String:MemberState
OfferReplicaBootstrapRequest ?EnvelopeHeader:Header
OfferReplicaBootstrapResponse Bool:Accepted String:MemberState String:RefusalCode
TransferReplicaStateRequest ?EnvelopeHeader:Header OperationId:TransferId UInt32:Sequence |ReplicaTransferBegin:Begin |ReplicaTransferSnapshot:Snapshot |ReplicaTransferWipe:Wipe |ReplicaTransferEntries:Entries |ReplicaTransferEnd:End |ReplicaTransferAbort:Abort
ReplicaTransferBegin UInt64:Index UInt64:IndexTerm UInt32:SnapshotBytes UInt32:SnapshotChecksum Bool:CeilingChecked
ReplicaTransferSnapshot UInt64:Offset Bytes:Chunk
ReplicaTransferWipe
ReplicaTransferEnd UInt64:EntryCount UInt32:EntriesCrc32C
ReplicaTransferAbort
TransferReplicaStateResponse UInt64:Term Bool:Success String:RefusalCode Bool:More
ReplicaVoteRequest ?EnvelopeHeader:Header UInt64:LastLogIndex UInt64:LastLogTerm
ReplicaVoteResponse UInt64:Term Bool:Granted String:RefusalCode
"""

PRIMITIVES = {
    "String": ("string", "Text"),
    "OperationId": ("string", "OperationId"),
    "Bytes": ("ReadOnlyMemory<byte>", "Bytes"),
    "TailBytes": ("ReadOnlyMemory<byte>", "TailBytes"),
    "Int64": ("long", "Signed"),
    "UInt32": ("uint", "UInt32"),
    "UInt64": ("ulong", "Unsigned"),
    "Double": ("double", "Double"),
    "Bool": ("bool", "Boolean"),
    "Timestamp": ("DateTime", "Timestamp"),
    "Duration": ("TimeSpan", "Duration"),
    "Null": ("NullValue", ""),
}
UNIONS = {"CacheValue": "Kind", "Trailer": "Value", "TransferReplicaStateRequest": "Step"}
OPERATIONS = """Unspecified SetEntry GetEntry GetValue Remove TryAddEntry Touch RemoveExpiration Update
GetExpiration GetOrAdd SetDistributed GetDistributed RefreshDistributed""".split()


def declaration(kind):
    if kind == "Trailer":
        return "Types.Trailer"
    return PRIMITIVES[kind][0] if kind in PRIMITIVES else kind


def read(kind):
    if kind == "Bytes":
        return "reader.Bytes(out _)"
    if kind == "TailBytes":
        return "reader.TailBytes(out _)"
    if kind == "Null":
        return "NullValue.NullValue"
    if kind in PRIMITIVES:
        return f"reader.{PRIMITIVES[kind][1]}()"
    return f"{declaration(kind)}.Read(ref reader)"


def write(kind, expression):
    if kind in PRIMITIVES:
        return f"writer.{PRIMITIVES[kind][1]}({expression});"
    return f"{expression}.Encode(ref writer);"


def model(name, fields, models, empty_response=False, status_body=False):
    lines = []
    add = lines.append
    mutation = any(kind == "?OperationId" for kind, _ in fields)
    base = "CacheMutationRequest" if mutation else "WireMessage"
    add(f"internal sealed class {name} : {base}")
    add("{")
    add(f"    internal static WireParser<{name}> Parser {{ get; }} = new(Read);")
    regular = [(kind, prop) for kind, prop in fields if kind[0] != "|"]
    variants = [(kind[1:], prop) for kind, prop in fields if kind[0] == "|"]
    flags = {}
    for kind, prop in regular:
        optional = kind.startswith("?")
        repeated = kind.startswith("*")
        kind = kind.lstrip("?*")
        type_name = declaration(kind)
        if optional or kind == "Bool":
            condition = f"{prop} != null" if optional else prop
            if optional and kind == "Int64":
                condition = f"Has{prop}"
            if optional and kind == "OperationId":
                condition = "operationId.Length != 0"
            flags[prop] = (condition, 1 << len(flags))
        if optional and kind == "OperationId":
            continue
        if repeated:
            add(f"    internal List<{type_name}> {prop} {{ get; }} = [];")
        elif optional and kind == "Int64":
            backing = "_" + prop[0].lower() + prop[1:]
            add(f"    private long? {backing};")
            add(f"    internal bool Has{prop} => {backing}.HasValue;")
            add(f"    internal long {prop} {{ get => {backing}.GetValueOrDefault(); set => {backing} = value; }}")
        else:
            default = " = string.Empty;" if kind in ("String", "OperationId") else ""
            nullable = "?" if optional else ""
            add(f"    internal {type_name}{nullable} {prop} {{ get; set; }}{default}")
    if variants:
        union = UNIONS[name]
        tag = union + "Case"
        enum = union + "OneofCase"
        add(f"    internal enum {enum} : byte {{ None, " + ", ".join(p for _, p in variants) + " }")
        add(f"    internal {enum} {tag} {{ get; private set; }}")
        if name == "TransferReplicaStateRequest":
            add("    private WireMessage? _step;")
        else:
            add("    private object? _reference;" if name == "CacheValue" else "    private string? _text;")
            add("    private ReadOnlyMemory<byte> _bytes;")
            if name == "CacheValue":
                add("    private long _bits;")
        for kind, prop in variants:
            case = f"{enum}.{prop}"
            if name == "TransferReplicaStateRequest":
                getter = f"{tag} == {case} ? ({kind})_step! : null!"
                setter = f"_step = value; {tag} = value == null ? {enum}.None : {case};"
            else:
                expressions = {
                    "Null": ("NullValue.NullValue", ""),
                    "String": ("_text ?? string.Empty", "_text = value;"),
                    "Bytes": (f"{tag} == {case} ? _bytes : default", "_bytes = value;"),
                    "Int64": (f"{tag} == {case} ? _bits : 0", "_bits = value;"),
                    "Double": (f"{tag} == {case} ? BitConverter.Int64BitsToDouble(_bits) : 0", "_bits = BitConverter.DoubleToInt64Bits(value);"),
                    "Bool": (f"{tag} == {case} && _bits != 0", "_bits = value ? 1 : 0;"),
                }
                getter, assign = expressions[kind]
                if name == "CacheValue" and kind == "String":
                    getter = "_reference as string ?? (EncodedTextMemory is { } bytes ? WireReader.Utf8.GetString(bytes.Span) : string.Empty)"
                    assign = "_reference = value;"
                clear = ("_text = null; " if kind != "String" else "") + ("_bytes = default; " if kind != "Bytes" else "")
                if name == "CacheValue":
                    clear = clear.replace("_text", "_reference")
                setter = f"{clear}{assign} {tag} = {case};".strip()
            add(f"    internal {declaration(kind)} {prop}")
            add("    {")
            add(f"        get => {getter};")
            add(f"        set {{ {setter} }}")
            add("    }")

    if name == "CacheValue":
        add("    internal byte[]? EncodedText => KindCase == KindOneofCase.StringValue ? _reference as byte[] : null;")
        add("    internal static CacheValue FromText(byte[] bytes) => new() { KindCase = KindOneofCase.StringValue, _reference = bytes };")
        # The reference slot holds text or the exact array of a parsed binary result.
        # Shared byte fields have no owner and retain their independent-copy semantics.
        add("")
        add("    internal ReadOnlyMemory<byte>? EncodedTextMemory")
        add("    {")
        add("        get")
        add("        {")
        add("            if (KindCase != KindOneofCase.StringValue || _reference is string)")
        add("                return null;")
        add("            return _reference is byte[] owned ? owned : _bytes;")
        add("        }")
        add("    }")
        add("")
        add("    internal static CacheValue FromTextSlice(ReadOnlyMemory<byte> bytes) => new() { KindCase = KindOneofCase.StringValue, _bytes = bytes };")
        add("")
        add("    internal static CacheValue FromOwnedBinary(byte[] payload) => new() { KindCase = KindOneofCase.BinaryValue, _bytes = payload, _reference = payload };")
        add("")
        add("    internal byte[] TakeBinaryValue()")
        add("    {")
        add("        if (KindCase != KindOneofCase.BinaryValue || _reference is not byte[] owned)")
        add("            return BinaryValue.ToArray();")
        add("        _reference = null;")
        add("        _bytes = default;")
        add("        return owned;")
        add("    }")
    add("")
    add("    internal override void Encode(ref WireWriter writer)")
    add("    {")
    if status_body:
        add("        if (Detail.Length == 0 && Trailers.Count == 0)")
        add("            return;")
    if mutation:
        add("        var operationId = writer.MutationOperationId ?? OperationId;")
    if flags:
        assert len(flags) <= 8
        condition = " | ".join(f"({condition} ? {bit} : 0)" for condition, bit in flags.values())
        if empty_response:
            add(f"        var flags = (byte)({condition});")
            empty = ["flags == 0"] + [f"{prop}.IsEmpty" for kind, prop in regular if kind in ("Bytes", "TailBytes")]
            add("        if (" + " && ".join(empty) + ")")
            add("            return;")
            add("        writer.Byte(flags);")
        else:
            add(f"        writer.Byte((byte)({condition}));")
    for kind, prop in regular:
        if kind == "Bool":
            continue
        if kind.startswith("*"):
            add(f"        writer.ItemCount({prop}.Count);")
            add(f"        foreach (var item in {prop})")
            add("            " + write(kind[1:], "item"))
        elif prop in flags:
            condition, _ = flags[prop]
            add(f"        if ({condition})")
            expr = prop + ".Value" if kind in ("?Duration", "?Timestamp") else prop
            if kind == "?OperationId":
                expr = "operationId"
            add("            " + write(kind[1:], expr))
        else:
            add("        " + write(kind, prop))
    if variants:
        add(f"        writer.Byte((byte){tag});")
        if name == "CacheValue":
            add("        EncodePayload(ref writer, false);")
            add("    }")
            add("")
            add("    internal void EncodePayload(ref WireWriter writer, bool tail)")
            add("    {")
        add(f"        switch ({tag})")
        add("        {")
        for kind, prop in variants:
            if kind != "Null":
                add(f"            case {enum}.{prop}:")
                if name == "CacheValue" and kind == "Bytes":
                    add("                if (tail)")
                    add(f"                    writer.TailBytes({prop});")
                    add("                else")
                    add(f"                    writer.Bytes({prop});")
                elif name == "CacheValue" and kind == "String":
                    add("                if (EncodedTextMemory is { } text)")
                    add("                {")
                    add("                    if (tail) writer.TailBytes(text);")
                    add("                    else writer.Bytes(text);")
                    add("                }")
                    add(f"                else writer.Text({prop}, tail);")
                else:
                    add("                " + write(kind, prop))
                add("                break;")
        add("        }")
    add("    }")
    add("")
    if name == "CacheValue":
        add(f"    internal static {name} Read(ref WireReader reader) => ReadPayload(ref reader, ({enum})reader.Byte(), false);")
        add("")
        add(f"    internal static {name} ReadPayload(ref WireReader reader, {enum} kind, bool tail)")
    else:
        add(f"    internal static {name} Read(ref WireReader reader)")
    add("    {")
    add(f"        var message = new {name}();")
    if (empty_response and flags) or status_body:
        add("        if (reader.Remaining == 0)")
        add("            return message;")
    if flags:
        add(f"        var flags = reader.Flags({(1 << len(flags)) - 1});")
    for kind, prop in regular:
        if kind.startswith("*"):
            add("        var count = reader.ItemCount();")
            add("        for (var i = 0; i < count; i++)")
            add(f"            message.{prop}.Add({read(kind[1:])});")
        elif prop in flags:
            bit = flags[prop][1]
            if kind == "Bool":
                add(f"        message.{prop} = (flags & {bit}) != 0;")
            else:
                add(f"        if ((flags & {bit}) != 0)")
                add(f"            message.{prop} = {read(kind[1:])};")
        else:
            add(f"        message.{prop} = {read(kind)};")
    if variants:
        add("        switch (kind)" if name == "CacheValue" else f"        switch (({enum})reader.Byte())")
        add("        {")
        add(f"            case {enum}.None:")
        add("                break;")
        for kind, prop in variants:
            add(f"            case {enum}.{prop}:")
            if name == "CacheValue" and prop == "BinaryValue":
                add("                message.BinaryValue = tail ? reader.TailBytes(out var owned) : reader.Bytes(out owned);")
                add("                message._reference = owned;")
            elif name == "CacheValue" and kind == "Bytes":
                add(f"                message.{prop} = tail ? reader.TailBytes(out _) : reader.Bytes(out _);")
            elif name == "CacheValue" and kind == "String":
                add("                message.KindCase = kind;")
                add("                message._reference = TextPayloadReader.Read(ref reader, tail);")
            else:
                add(f"                message.{prop} = {read(kind)};")
            add("                break;")
        add("            default:")
        add('                throw new InvalidDataException("Unknown union discriminator.");')
        add("        }")
    add("        return message;")
    add("    }")
    if name == "BinaryCallError":
        add("")
        add("    internal static class Types")
        add("    {")
        lines.extend("        " + line if line else "" for line in model("Trailer", models["Trailer"], models))
        add("    }")
    add("}")
    return lines


def generate(schema, namespace, path, cache=False):
    models = {}
    for line in schema.strip().splitlines():
        name, *fields = line.split()
        models[name] = [field.split(":") for field in fields]
    lines = ["// <auto-generated />", "#nullable enable", "using System;", "using System.Collections.Generic;",
             "using System.IO;", "using Squirix.Transport.Wire.Codec;", "", f"namespace {namespace};", ""]
    if cache:
        lines += ["internal enum NullValue : byte { NullValue }", "", "internal enum BinaryOperation", "{"]
        lines += [f"    {name} = {value}," for value, name in enumerate(OPERATIONS)]
        lines += ["}", "", "internal abstract class CacheMutationRequest : WireMessage", "{",
                  "    internal string OperationId { get; set; } = string.Empty;", "}", ""]
    nested = {kind.lstrip("?*|") for fields in models.values() for kind, _ in fields}
    for name, fields in models.items():
        if name != "Trailer":
            for index, (kind, _) in enumerate(fields):
                if kind.lstrip("?*|") == "TailBytes":
                    assert kind == "TailBytes" and name not in nested and index == len(fields) - 1
            # Only a whole response has a frame boundary that can represent default fields.
            empty_response = cache and name.endswith("AsyncResponse") and name not in nested and all(
                kind in ("Bool", "Bytes", "TailBytes") or kind.startswith("?") for kind, _ in fields
            )
            status_body = name == "BinaryCallError" and name not in nested
            lines += model(name, fields, models, empty_response, status_body) + [""]
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    generate(CACHE, "Squirix.Transport.Wire.Cache", SHARED / "CacheMessages.g.cs", cache=True)
    generate(REPLICATION, "Squirix.Server.Adapters.Wire.Replication", REPLICA / "ReplicaMessages.g.cs")
