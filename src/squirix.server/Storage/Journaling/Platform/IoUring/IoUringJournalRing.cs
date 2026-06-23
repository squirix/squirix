#pragma warning disable MA0181 // io_uring mmap layout requires native pointer arithmetic.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;

namespace Squirix.Server.Storage.Journaling.Platform.IoUring;

/// <summary>Single-issuer io_uring ring for synchronous journal segment writes.</summary>
[SupportedOSPlatform("linux")]
internal sealed class IoUringJournalRing : IDisposable
{
    private const int SqeSizeBytes = 64;
    private const int CqeSizeBytes = 16;
    private readonly int _ringFd;
    private readonly uint _sqMask;
    private readonly uint _cqMask;
    private readonly nint _sqRing;
    private readonly nint _cqRing;
    private readonly nint _sqes;
    private readonly nuint _sqRingSize;
    private readonly nuint _cqRingSize;
    private readonly nuint _sqesSize;
    private readonly unsafe uint* _sqTail;
    private readonly unsafe uint* _sqArray;
    private readonly unsafe LinuxIoUringSyscalls.IoUringSqe* _sqeEntries;
    private readonly unsafe uint* _cqHead;
    private readonly unsafe uint* _cqTail;
    private readonly unsafe LinuxIoUringSyscalls.IoUringCqe* _cqes;
    private uint _sqTailLocal;
    private bool _disposed;

    internal IoUringJournalRing(uint entries)
    {
        if (entries == 0 || (entries & (entries - 1)) != 0)
            throw new ArgumentOutOfRangeException(nameof(entries), entries, "Ring size must be a power of two.");

        var parameters = new LinuxIoUringSyscalls.IoUringParams { SqEntries = entries, CqEntries = entries * 2 };
        _ringFd = LinuxIoUringSyscalls.IoUringSetup(entries, ref parameters);
        if (_ringFd < 0)
            throw new IOException($"io_uring_setup failed with errno {Marshal.GetLastPInvokeError()}.");

        _sqMask = parameters.SqEntries - 1;
        _cqMask = parameters.CqEntries - 1;

        _sqRingSize = parameters.SqOff.Array + (nuint)(parameters.SqEntries * sizeof(uint));
        _cqRingSize = parameters.CqOff.Cqes + (nuint)(parameters.CqEntries * CqeSizeBytes);
        _sqesSize = parameters.SqEntries * SqeSizeBytes;

        _sqRing = LinuxIoUringSyscalls.Mmap(0, _sqRingSize, LinuxIoUringSyscalls.ProtReadWrite, LinuxIoUringSyscalls.MapShared, _ringFd, (long)LinuxIoUringSyscalls.OffSqRing);
        _cqRing = LinuxIoUringSyscalls.Mmap(0, _cqRingSize, LinuxIoUringSyscalls.ProtReadWrite, LinuxIoUringSyscalls.MapShared, _ringFd, (long)LinuxIoUringSyscalls.OffCqRing);
        _sqes = LinuxIoUringSyscalls.Mmap(0, _sqesSize, LinuxIoUringSyscalls.ProtReadWrite, LinuxIoUringSyscalls.MapShared, _ringFd, (long)LinuxIoUringSyscalls.OffSqes);

        if (_sqRing == -1 || _cqRing == -1 || _sqes == -1)
            throw new IOException($"io_uring mmap failed with errno {Marshal.GetLastPInvokeError()}.");

        unsafe
        {
            _sqTail = (uint*)nint.Add(_sqRing, checked((int)parameters.SqOff.Tail));
            _sqArray = (uint*)nint.Add(_sqRing, checked((int)parameters.SqOff.Array));
            _sqeEntries = (LinuxIoUringSyscalls.IoUringSqe*)_sqes;
            _cqHead = (uint*)nint.Add(_cqRing, checked((int)parameters.CqOff.Head));
            _cqTail = (uint*)nint.Add(_cqRing, checked((int)parameters.CqOff.Tail));
            _cqes = (LinuxIoUringSyscalls.IoUringCqe*)nint.Add(_cqRing, checked((int)parameters.CqOff.Cqes));
            _sqTailLocal = Volatile.Read(ref *_sqTail);
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_sqRing != -1)
            _ = LinuxIoUringSyscalls.Munmap(_sqRing, _sqRingSize);

        if (_cqRing != -1)
            _ = LinuxIoUringSyscalls.Munmap(_cqRing, _cqRingSize);

        if (_sqes != -1)
            _ = LinuxIoUringSyscalls.Munmap(_sqes, _sqesSize);

        if (_ringFd >= 0)
            _ = LinuxIoUringSyscalls.Close(_ringFd);
    }

    internal void Write(int fileDescriptor, ReadOnlySpan<byte> buffer, long fileOffset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        unsafe
        {
            var native = (byte*)NativeMemory.Alloc((nuint)buffer.Length);
            try
            {
                buffer.CopyTo(new Span<byte>(native, buffer.Length));
                EnqueueWrite(fileDescriptor, (ulong)native, (uint)buffer.Length, (ulong)fileOffset);
                SubmitAndWait();
            }
            finally
            {
                NativeMemory.Free(native);
            }
        }
    }

    internal void Fsync(int fileDescriptor)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnqueueFsync(fileDescriptor);
        SubmitAndWait();
    }

    internal void WriteManifestRoll(int dataFd, ReadOnlySpan<byte> data, int pointerFd, ReadOnlySpan<byte> pointer)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        unsafe
        {
            var dataNative = (byte*)NativeMemory.Alloc((nuint)data.Length);
            var pointerNative = (byte*)NativeMemory.Alloc((nuint)pointer.Length);
            try
            {
                data.CopyTo(new Span<byte>(dataNative, data.Length));
                pointer.CopyTo(new Span<byte>(pointerNative, pointer.Length));
                var sqTailBefore = _sqTailLocal;
                EnqueueWrite(dataFd, (ulong)dataNative, (uint)data.Length, 0);
                EnqueueWrite(pointerFd, (ulong)pointerNative, (uint)pointer.Length, 0);
                EnqueueFsync(dataFd);
                EnqueueFsync(pointerFd);
                SubmitBatchAndWait(_sqTailLocal - sqTailBefore, 4);
            }
            finally
            {
                NativeMemory.Free(dataNative);
                NativeMemory.Free(pointerNative);
            }
        }
    }

    private unsafe void EnqueueWrite(int fileDescriptor, ulong bufferAddress, uint length, ulong offset)
    {
        ref var sqe = ref ReserveSqe();
        sqe.Opcode = LinuxIoUringSyscalls.OpWrite;
        sqe.Fd = fileDescriptor;
        sqe.Off = offset;
        sqe.Addr = bufferAddress;
        sqe.Len = length;
        _sqTailLocal++;
    }

    private unsafe void EnqueueFsync(int fileDescriptor)
    {
        ref var sqe = ref ReserveSqe();
        sqe.Opcode = LinuxIoUringSyscalls.OpFsync;
        sqe.Fd = fileDescriptor;
        sqe.FsyncFlags = LinuxIoUringSyscalls.FsyncDatasync;
        _sqTailLocal++;
    }

    /// <summary>
    /// Reserves the SQE for the current tail slot and maps it through the submission-queue index array.
    /// The kernel-mmap'd <c>array</c> is zero-initialized, so it must be populated explicitly: without
    /// this, every entry in a multi-SQE batch resolves to <c>sqe[0]</c> and only the last enqueued
    /// operation survives (silently dropping the data/pointer writes in <see cref="WriteManifestRoll" />).
    /// </summary>
    private unsafe ref LinuxIoUringSyscalls.IoUringSqe ReserveSqe()
    {
        var slot = _sqTailLocal & _sqMask;
        _sqArray[slot] = slot;
        ref var sqe = ref Unsafe.Add(ref *_sqeEntries, (int)slot);
        sqe = default;
        return ref sqe;
    }

    private unsafe void SubmitAndWait()
    {
        Volatile.Write(ref *_sqTail, _sqTailLocal);
        Thread.MemoryBarrier();

        var enterResult = LinuxIoUringSyscalls.IoUringEnter(_ringFd, 1, 1, LinuxIoUringSyscalls.EnterGetEvents);
        if (enterResult < 0)
            throw new IOException($"io_uring_enter failed with errno {Marshal.GetLastPInvokeError()}.");

        var head = Volatile.Read(ref *_cqHead);
        var tail = Volatile.Read(ref *_cqTail);
        while (head == tail)
        {
            enterResult = LinuxIoUringSyscalls.IoUringEnter(_ringFd, 0, 1, LinuxIoUringSyscalls.EnterGetEvents);
            if (enterResult < 0)
                throw new IOException($"io_uring_enter wait failed with errno {Marshal.GetLastPInvokeError()}.");

            head = Volatile.Read(ref *_cqHead);
            tail = Volatile.Read(ref *_cqTail);
        }

        var cqeIndex = head & _cqMask;
        ref var cqe = ref Unsafe.Add(ref *_cqes, (int)cqeIndex);
        if (cqe.Res < 0)
            throw new IOException($"io_uring completion failed with code {cqe.Res}.");

        Volatile.Write(ref *_cqHead, head + 1);
    }

    private unsafe void SubmitBatchAndWait(uint sqesToSubmit, uint expectedCompletions)
    {
        Volatile.Write(ref *_sqTail, _sqTailLocal);
        Thread.MemoryBarrier();

        var submitted = false;
        var completed = 0U;
        while (completed < expectedCompletions)
        {
            var toSubmit = submitted ? 0U : sqesToSubmit;
            var minComplete = expectedCompletions - completed;
            var enterResult = LinuxIoUringSyscalls.IoUringEnter(_ringFd, toSubmit, minComplete, LinuxIoUringSyscalls.EnterGetEvents);
            if (enterResult < 0)
                throw new IOException($"io_uring_enter failed with errno {Marshal.GetLastPInvokeError()}.");

            submitted = true;

            var head = Volatile.Read(ref *_cqHead);
            var tail = Volatile.Read(ref *_cqTail);
            while (head != tail && completed < expectedCompletions)
            {
                var cqeIndex = head & _cqMask;
                ref var cqe = ref Unsafe.Add(ref *_cqes, (int)cqeIndex);
                if (cqe.Res < 0)
                    throw new IOException($"io_uring completion failed with code {cqe.Res}.");

                head++;
                completed++;
            }

            Volatile.Write(ref *_cqHead, head);
        }
    }
}

#pragma warning restore MA0181
