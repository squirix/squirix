using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Searches for a legal sequential register history respecting real-time precedence, independently for each key.</summary>
internal static class RegisterLinearizability
{
    /// <summary>Returns violations after the fast single-writer precheck and an exhaustive memoized search.</summary>
    /// <param name="writes">Acknowledged and ambiguous writes with unique, increasing positive values per key.</param>
    /// <param name="reads">Successful reads; zero denotes the initial state.</param>
    /// <param name="cancellationToken">Cancels the search without reporting a verdict.</param>
    /// <returns>One or more violations, or an empty list only when a legal order exists for every key.</returns>
    /// <exception cref="ArgumentException">A read ends before it starts.</exception>
    internal static List<string> Check(IReadOnlyList<RegisterWrite> writes, IReadOnlyList<RegisterRead> reads, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var violations = RegisterHistory.Check(writes, reads);
        if (violations.Count != 0)
            return violations;

        var keys = new SortedDictionary<string, List<Operation>>(StringComparer.Ordinal);
        for (var i = 0; i < reads.Count; i++)
        {
            var read = reads[i];
            if (read.End < read.Start)
                throw new ArgumentException("A read ends before it starts.", nameof(reads));
            Add(read.Key, new Operation(read.Start, read.End, read.Observed, true, true));
        }

        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];

            // A timeout is not a completion: the request may apply arbitrarily late, or be omitted.
            Add(write.Key, new Operation(write.Start, write.Acked ? write.End : long.MaxValue, write.Value, false, write.Acked));
        }

        foreach (var (key, operations) in keys)
        {
            if (!new Search([.. operations], cancellationToken).Run())
                violations.Add($"key {key}: no linearization exists for the complete history");
        }

        return violations;

        void Add(string key, Operation operation)
        {
            if (!keys.TryGetValue(key, out var operations))
            {
                operations = [];
                keys.Add(key, operations);
            }

            operations.Add(operation);
        }
    }

    private readonly record struct Operation(long Start, long End, long Value, bool Read, bool Required);

    private sealed class Search
    {
        private readonly Operation[] _operations;
        private readonly CancellationToken _cancellationToken;
        private readonly HashSet<(BigInteger Remaining, long Value)> _rejected = [];
        private readonly bool[] _used;

        internal Search(Operation[] operations, CancellationToken cancellationToken)
        {
            _operations = operations;
            _cancellationToken = cancellationToken;
            _used = new bool[operations.Length];
            Array.Sort(_operations, static (a, b) => a.End.CompareTo(b.End));
        }

        internal bool Run()
        {
            var remaining = (BigInteger.One << _operations.Length) - 1;
            var required = 0;
            foreach (var operation in _operations)
                required += operation.Required ? 1 : 0;
            var frames = new Stack<Frame>();
            frames.Push(new Frame(0, 0, -1));
            while (frames.Count != 0)
            {
                _cancellationToken.ThrowIfCancellationRequested();
                if (required == 0)
                    return true;
                if (Advance(frames, ref remaining, ref required))
                    continue;
                var frame = frames.Pop();
                _ = _rejected.Add((remaining, frame.Value));
                if (frame.Applied < 0)
                    continue;
                _used[frame.Applied] = false;
                remaining |= BigInteger.One << frame.Applied;
                required += _operations[frame.Applied].Required ? 1 : 0;
            }

            return false;
        }

        private bool Advance(Stack<Frame> frames, ref BigInteger remaining, ref int required)
        {
            var frame = frames.Peek();
            var earliestEnd = _operations[frame.First].End;
            while (frame.Next < _operations.Length)
            {
                var index = frame.Next++;
                var operation = _operations[index];

                // Equal endpoints overlap. The earliest remaining response bounds the next eligible invocation.
                if (_used[index] || operation.Start > earliestEnd || (operation.Read && operation.Value != frame.Value))
                    continue;
                var next = remaining ^ (BigInteger.One << index);
                var value = operation.Read ? frame.Value : operation.Value;
                if (_rejected.Contains((next, value)))
                    continue;
                _used[index] = true;
                remaining = next;
                required -= operation.Required ? 1 : 0;
                var first = frame.First;
                while (first < _used.Length && _used[first])
                    first++;
                frames.Push(new Frame(value, first, index));
                return true;
            }

            return false;
        }

        private sealed class Frame
        {
            internal Frame(long value, int first, int applied)
            {
                Value = value;
                First = first;
                Next = first;
                Applied = applied;
            }

            internal long Value { get; }

            internal int First { get; }

            internal int Applied { get; }

            internal int Next { get; set; }
        }
    }
}
