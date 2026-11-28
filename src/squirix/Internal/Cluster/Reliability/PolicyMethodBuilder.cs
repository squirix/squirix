using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Squirix.Internal.Cluster.Reliability;

/// <summary>Reuses suspended policy calls across the submitting thread and the socket completion threads.</summary>
/// <typeparam name="T">The result delivered by the policy.</typeparam>
internal struct PolicyMethodBuilder<T>
{
    private AsyncValueTaskMethodBuilder<T> _immediate;
    private Completion? _completion;

#pragma warning disable SA1415 // The C# compiler requires these members to be public even for an internal async method builder.
    public ValueTask<T> Task => _completion?.Operation ?? _immediate.Task;

    public static PolicyMethodBuilder<T> Create() => new() { _immediate = AsyncValueTaskMethodBuilder<T>.Create() };

    public void Start<TState>(ref TState machine)
        where TState : IAsyncStateMachine =>
        _immediate.Start(ref machine);

    public void SetStateMachine(IAsyncStateMachine machine) => _immediate.SetStateMachine(machine);

    public void SetResult(T value)
    {
        if (_completion is { } completion)
            completion.SetResult(value);
        else
            _immediate.SetResult(value);
    }

    public void SetException(Exception error)
    {
        if (_completion is { } completion)
        {
            completion.SetException(error);
            return;
        }

        _immediate.SetException(error);
    }

    public void AwaitOnCompleted<TAwaiter, TState>(ref TAwaiter awaiter, ref TState machine)
        where TAwaiter : INotifyCompletion
        where TState : IAsyncStateMachine =>
        awaiter.OnCompleted(Capture(ref machine));

    public void AwaitUnsafeOnCompleted<TAwaiter, TState>(ref TAwaiter awaiter, ref TState machine)
        where TAwaiter : ICriticalNotifyCompletion
        where TState : IAsyncStateMachine =>
        awaiter.UnsafeOnCompleted(Capture(ref machine));

#pragma warning restore SA1415

    private Action Capture<TState>(ref TState machine)
        where TState : IAsyncStateMachine
    {
        if (_completion is not Box<TState> box)
        {
            box = Box<TState>.Rent();
            _completion = box;
            box.Initialize(ref machine);
        }

        box.Context = ExecutionContext.Capture();
        return box.Resume;
    }

    private abstract class Completion : IValueTaskSource<T>
    {
        private ManualResetValueTaskSourceCore<T> _result = new() { RunContinuationsAsynchronously = false };

        internal ValueTask<T> Operation => new(this, _result.Version);

        public ValueTaskSourceStatus GetStatus(short token) => _result.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _result.OnCompleted(continuation, state, token, flags);

        public T GetResult(short token)
        {
            if (_result.GetStatus(token) == ValueTaskSourceStatus.Pending)
                throw new InvalidOperationException("The operation is still in progress.");
            try
            {
                return _result.GetResult(token);
            }
            finally
            {
                _result.Reset();
                Recycle();
            }
        }

        internal void SetResult(T value) => _result.SetResult(value);

        internal void SetException(Exception error) => _result.SetException(error);

        protected abstract void Recycle();
    }

    private sealed class Box<TState> : Completion
        where TState : IAsyncStateMachine
    {
        private readonly bool _pooled;
        private TState? _machine;

        private Box(bool pooled)
        {
            _pooled = pooled;
            Resume = MoveNext;
        }

        internal Action Resume { get; }

        internal ExecutionContext? Context { get; set; }

#pragma warning disable S2743 // Each closed compiler state-machine type requires its own pool of strongly typed boxes.
        private static Pool Available { get; } = new();
#pragma warning restore S2743

        internal static Box<TState> Rent() => Available.Acquire();

        internal void Initialize(ref TState machine) => _machine = machine;

        protected override void Recycle()
        {
            Context = null;
            _machine = default;
            if (_pooled)
                Available.Idle.Enqueue(this);
        }

        private void MoveNext()
        {
            var context = Context;
            if (context == null || ReferenceEquals(context, ExecutionContext.Capture()))
            {
                var starter = AsyncTaskMethodBuilder.Create();
                starter.Start(ref _machine!);
            }
            else
            {
                ExecutionContext.Run(context, static state => (state as Box<TState>)!._machine!.MoveNext(), this);
            }
        }

        private sealed class Pool
        {
            private int _reserved;

            internal ConcurrentQueue<Box<TState>> Idle { get; } = new();

            internal Box<TState> Acquire() =>
                Idle.TryDequeue(out var box) ? box : new Box<TState>(Volatile.Read(ref _reserved) < 128 && Interlocked.Increment(ref _reserved) <= 128);
        }
    }
}
