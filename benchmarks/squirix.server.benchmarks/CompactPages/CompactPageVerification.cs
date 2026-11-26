using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Benchmarks.CompactPages;

/// <summary>Checks mixed typed values in compact storage without collecting performance measurements.</summary>
internal static class CompactPageVerification
{
    private const int KeyCount = 2_000_000;

    internal static void Run()
    {
        CheckPayloadOwnership(new CompactPageCache());
        var cache = new CompactPageCache();
        for (var id = 0; id < KeyCount; id++)
            Write(cache, id, false);
        VerifyAll(cache, false, false);

        // Across the population each original kind is replaced by every kind, including itself.
        for (var id = 0; id < KeyCount; id++)
            Write(cache, id, true);
        VerifyAll(cache, true, false);

        for (var id = 0; id < KeyCount; id += 2)
        {
            var unused = default(StoredValue);
            if (!cache.Access(Key(id), CompactPageCache.Operation.Remove, ref unused))
                throw new InvalidOperationException($"Could not remove {id}.");
        }

        VerifyAll(cache, true, true);
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"PASS: {KeyCount} mixed entries, 49 kind transitions, {KeyCount / 2} removals, {3 * KeyCount} verified lookups."));
    }

    private static string Key(int id) => id.ToString("X8", CultureInfo.InvariantCulture);

    private static void Write(CompactPageCache cache, int id, bool replaced)
    {
        var value = Expected(id, replaced);
        cache.Access(Key(id), CompactPageCache.Operation.Set, ref value);
    }

    private static StoredValue Expected(int id, bool replaced)
    {
        var kind = (replaced ? id + (id / 7) + 1 : id) % 7;
        var number = replaced ? ~id : id;
        return kind switch
        {
            0 => default,
            1 => StoredValue.FromBoolean((number & 1) == 0),
            2 => StoredValue.FromInteger((id % 4) switch { 0 => long.MinValue, 1 => long.MaxValue, _ => number }),
            3 => StoredValue.FromDouble((id % 5) switch
            {
                0 => -0.0,
                1 => BitConverter.Int64BitsToDouble(0x7FF8000000000042L),
                2 => double.NegativeInfinity,
                _ => number + 0.125,
            }),
            4 => StoredValue.FromText(id % 19 == 0 ? string.Empty : "я" + Key(number)),
            5 => StoredValue.FromBinary(Binary(number)),
            _ => StoredValue.FromJson(Encoding.UTF8.GetBytes("{\"id\":\"" + Key(number) + "\",\"v\":1,\"v\":2}")),
        };
    }

    private static byte[] Binary(int number)
    {
        if (number % 17 == 0)
            return [];
        var bytes = GC.AllocateUninitializedArray<byte>(sizeof(int));
        BinaryPrimitives.WriteInt32LittleEndian(bytes, number);
        return bytes;
    }

    private static void VerifyAll(CompactPageCache cache, bool replaced, bool removedEven)
    {
        Parallel.For(0, 8, partition =>
        {
            for (var id = partition; id < KeyCount; id += 8)
            {
                var actual = default(StoredValue);
                var found = cache.Access(Key(id), CompactPageCache.Operation.Get, ref actual);
                var expectedFound = !removedEven || (id & 1) != 0;
                if (found != expectedFound || (found && !Matches(in actual, Expected(id, replaced))))
                    throw new InvalidOperationException($"Incorrect presence, kind or value at key {id}.");
            }
        });
    }

    private static bool Matches(in StoredValue actual, in StoredValue expected) =>
        actual.Kind == expected.Kind && expected.Kind switch
        {
            StoredValueKind.Null => true,
            StoredValueKind.Boolean => actual.Boolean == expected.Boolean,
            StoredValueKind.Integer => actual.Integer == expected.Integer,
            StoredValueKind.Double => BitConverter.DoubleToInt64Bits(actual.Double) == BitConverter.DoubleToInt64Bits(expected.Double),
            _ => actual.Bytes.AsSpan().SequenceEqual(expected.Bytes),
        };

    private static void CheckPayloadOwnership(CompactPageCache cache)
    {
        const string key = "payload";
        foreach (var size in new[] { 0, 1, 254, 255, 256, 4096, 65536 })
        {
            var bytes = new byte[size];
            Array.Fill(bytes, byte.MaxValue);
            var value = StoredValue.FromBinary(bytes);
            cache.Access(key, CompactPageCache.Operation.Set, ref value);
            Array.Clear(bytes);
            var held = default(StoredValue);
            if (!cache.Access(key, CompactPageCache.Operation.Get, ref held))
                throw new InvalidOperationException("Expected binary value.");
            value = default;
            cache.Access(key, CompactPageCache.Operation.Set, ref value);
            if (held.Kind != StoredValueKind.Binary || held.Bytes.Length != size || held.Bytes.AsSpan().IndexOfAnyExcept(byte.MaxValue) >= 0)
                throw new InvalidOperationException("Binary value lost ownership across replacement.");
        }
    }
}
