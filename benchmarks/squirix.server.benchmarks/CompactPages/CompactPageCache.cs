using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Core;

namespace Squirix.Server.Benchmarks;

/// <summary>Experimental physical storage for ordinary typed entries in one namespace.</summary>
internal sealed class CompactPageCache
{
    private const int ShardBits = 6;
    private static readonly UTF8Encoding Utf8 = new(false, true);
    private readonly Shard[] _shards = new Shard[1 << ShardBits];

    internal CompactPageCache()
    {
        for (var i = 0; i < _shards.Length; i++)
            _shards[i] = new Shard();
    }

    internal enum Operation
    {
        /// <summary>Read an independent value.</summary>
        Get = 0,

        /// <summary>Insert or replace a value.</summary>
        Set = 1,

        /// <summary>Delete a key.</summary>
        Remove = 2,
    }

    internal bool Access(string key, Operation operation, ref StoredValue value)
    {
        var length = Utf8.GetByteCount(key);
        Span<byte> bytes = length <= 256 ? stackalloc byte[length] : new byte[length];
        Utf8.GetBytes(key, bytes);
        var hash = KeyHash(bytes);
        var shard = _shards[hash & (_shards.Length - 1)];
        lock (shard)
            return shard.Access(hash, bytes, operation, ref value);
    }

    private static int KeyHash(ReadOnlySpan<byte> key)
    {
        var hash = default(HashCode);
        hash.AddBytes(key);
        return hash.ToHashCode();
    }

    [StructLayout(LayoutKind.Auto)]
    private struct Bucket
    {
        internal int Hash;
        internal int Handle;
    }

    private sealed class Shard
    {
        private readonly Bank?[] _banks = new Bank[64];
        private Bucket[] _index = new Bucket[16];
        private int _count;

        internal bool Access(int hash, ReadOnlySpan<byte> key, Operation op, ref StoredValue value)
        {
            var slot = Find(hash, key);
            var handle = _index[slot].Handle;
            if (op == Operation.Get)
            {
                if (handle == 0)
                    return false;

                var record = Record(handle);
                value = CompactPageValue.Read(record[(sizeof(int) + key.Length)..]);
                return true;
            }

            if (op == Operation.Remove)
            {
                if (handle == 0)
                    return false;
                Release(handle);
                CloseHole(slot);
                --_count;
                if (_index.Length > 16 && _count <= _index.Length / 5)
                    Resize(_index.Length / 2);
                return true;
            }

            Set(hash, key, in value, slot, handle);
            return true;
        }

        private static int SizeClass(int size) => size <= 256 ? (size - 1) / 8 : BitOperations.Log2(uint.CreateChecked(size - 1)) + 24;

        private static int CellSize(int sizeClass) => sizeClass < 32 ? (sizeClass + 1) * 8 : int.CreateChecked(1u << (sizeClass - 23));

        private void Set(int hash, ReadOnlySpan<byte> key, in StoredValue value, int slot, int handle)
        {
            var sizeClass = SizeClass(checked(sizeof(int) + key.Length + CompactPageValue.EncodedLength(in value)));
            if (handle == 0 || (handle & 63) != sizeClass)
            {
                if (handle == 0 && _count >= _index.Length - (_index.Length / 4))
                {
                    Resize(checked(_index.Length * 2));
                    slot = Find(hash, key);
                }

                var bank = _banks[sizeClass] ??= new Bank(CellSize(sizeClass));
                var newHandle = bank.Allocate(sizeClass);
                if (handle != 0)
                    Release(handle);
                else
                    _count++;
                _index[slot] = new Bucket { Hash = hash, Handle = newHandle };
                handle = newHandle;
            }

            var target = Record(handle);
            BinaryPrimitives.WriteInt32LittleEndian(target, key.Length);
            key.CopyTo(target[sizeof(int)..]);
            CompactPageValue.Write(target[(sizeof(int) + key.Length)..], in value);
        }

        private Span<byte> Record(int handle) => _banks[handle & 63]!.At((handle >>> 6) - 1);

        private int Find(int hash, ReadOnlySpan<byte> key)
        {
            var mask = _index.Length - 1;
            var slot = (hash >>> ShardBits) & mask;
            while (_index[slot].Handle != 0)
            {
                if (_index[slot].Hash == hash)
                {
                    var record = Record(_index[slot].Handle);
                    if (BinaryPrimitives.ReadInt32LittleEndian(record) == key.Length && record.Slice(sizeof(int), key.Length).SequenceEqual(key))
                        break;
                }

                slot = (slot + 1) & mask;
            }

            return slot;
        }

        private void Release(int handle)
        {
            var bank = _banks[handle & 63]!;
            var hole = (handle >>> 6) - 1;
            if (hole != bank.Count - 1)
            {
                // Move the last live record into the hole so random deletions also free whole pages.
                var last = bank.At(bank.Count - 1);
                var key = last.Slice(sizeof(int), BinaryPrimitives.ReadInt32LittleEndian(last));
                var movedSlot = Find(KeyHash(key), key);
                last.CopyTo(bank.At(hole));
                _index[movedSlot].Handle = handle;
            }

            bank.DropLast();
        }

        private void CloseHole(int hole)
        {
            var mask = _index.Length - 1;
            for (var pos = (hole + 1) & mask; _index[pos].Handle != 0; pos = (pos + 1) & mask)
            {
                var home = (_index[pos].Hash >>> ShardBits) & mask;
                if (((hole - home) & mask) >= ((pos - home) & mask))
                    continue;
                _index[hole] = _index[pos];
                hole = pos;
            }

            _index[hole] = default;
        }

        private void Resize(int length)
        {
            var next = new Bucket[length];
            foreach (var entry in _index)
            {
                if (entry.Handle == 0)
                    continue;
                var pos = (entry.Hash >>> ShardBits) & (length - 1);
                while (next[pos].Handle != 0)
                    pos = (pos + 1) & (length - 1);
                next[pos] = entry;
            }

            _index = next;
        }
    }

    private sealed class Bank
    {
        private readonly int _cellSize;
        private readonly int _perPage;
        private readonly List<byte[]> _pages = [];

        internal Bank(int cellSize)
        {
            _cellSize = cellSize;
            _perPage = Math.Max(1, 16384 / cellSize);
        }

        internal int Count { get; private set; }

        internal int Allocate(int sizeClass)
        {
            var handle = checked(((Count + 1) * 64) + sizeClass);
            if (Count % _perPage == 0)
                _pages.Add(new byte[_perPage * _cellSize]);
            Count++;
            return handle;
        }

        internal Span<byte> At(int index) => _pages[index / _perPage].AsSpan((index % _perPage) * _cellSize, _cellSize);

        internal void DropLast()
        {
            if (--Count % _perPage == 0)
                _pages.RemoveAt(_pages.Count - 1);
        }
    }
}
