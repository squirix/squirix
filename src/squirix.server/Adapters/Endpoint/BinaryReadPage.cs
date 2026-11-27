using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Keeps a pipe segment alive while dispatched calls are reading its frames.</summary>
internal sealed class BinaryReadPage : MemoryManager<byte>
{
    internal static readonly MemoryPool<byte> Pool = new PagePool();

    private int _references;

    private BinaryReadPage()
    {
    }

    internal byte[] Bytes { get; private set; } = [];

    public override Span<byte> GetSpan() => Bytes;

    public override MemoryHandle Pin(int elementIndex = 0) => Bytes.AsMemory(elementIndex).Pin();

    public override void Unpin()
    {
        // The array's MemoryHandle owns its pin.
    }

    internal static (BinaryReadPage? Page, int Offset) Find(in ReadOnlySequence<byte> buffer) =>
        buffer.IsSingleSegment ? Find(buffer.First) : default;

    internal static (BinaryReadPage? Page, int Offset) Find(ReadOnlyMemory<byte> memory) =>
        MemoryMarshal.TryGetMemoryManager<byte, BinaryReadPage>(memory, out var page, out var offset, out _) ? (page, offset) : default;

    internal void Retain() => Interlocked.Increment(ref _references);

    internal void Release()
    {
        if (Interlocked.Decrement(ref _references) != 0)
            return;

        PagePool.Return(this);
    }

    protected override bool TryGetArray(out ArraySegment<byte> segment)
    {
        segment = new ArraySegment<byte>(Bytes);
        return true;
    }

    protected override void Dispose(bool disposing) => Release();

    private sealed class PagePool : MemoryPool<byte>
    {
        [ThreadStatic]
        private static BinaryReadPage? _spare;

        public override int MaxBufferSize => Array.MaxLength;

        public override IMemoryOwner<byte> Rent(int minBufferSize = -1) =>
            RentPage(minBufferSize == -1 ? 4096 : minBufferSize);

        internal static void Return(BinaryReadPage page)
        {
            ArrayPool<byte>.Shared.Return(page.Bytes);
            page.Bytes = [];
            _spare ??= page;
        }

        protected override void Dispose(bool disposing)
        {
            // The shared array pool owns the backing storage.
        }

        private static BinaryReadPage RentPage(int size)
        {
            var page = _spare ?? new BinaryReadPage();
            _spare = null;
            page.Bytes = ArrayPool<byte>.Shared.Rent(size);
            page._references = 1;
            return page;
        }
    }
}
