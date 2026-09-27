using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>In-memory <see cref="IAsyncStreamReader{T}" /> backed by a fixed item sequence.</summary>
/// <typeparam name="T">The streamed message type.</typeparam>
[Immutable]
internal sealed class TestAsyncStreamReader<T> : IAsyncStreamReader<T>
{
    private readonly IEnumerator<T> _items;

    /// <summary>Initializes a new instance of the <see cref="TestAsyncStreamReader{T}" /> class.</summary>
    /// <param name="items">The item sequence to stream.</param>
    internal TestAsyncStreamReader(IEnumerable<T> items)
    {
        _items = items.GetEnumerator();
    }

    /// <summary>Initializes a new instance of the <see cref="TestAsyncStreamReader{T}" /> class streaming a single item.</summary>
    /// <param name="item">The item to stream.</param>
    internal TestAsyncStreamReader(T item)
        : this([item])
    {
    }

    /// <inheritdoc />
    public T Current => _items.Current;

    /// <inheritdoc />
    public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(_items.MoveNext());
}
