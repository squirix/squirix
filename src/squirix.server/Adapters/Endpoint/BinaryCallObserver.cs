using System;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Observes admitted binary calls inside their correlation and deadline scope.</summary>
/// <remarks>
/// Entry and exit callbacks can run on the connection reader. Synchronous callbacks must complete promptly.
/// Use the asynchronous entry callback for work that awaits I/O.
/// </remarks>
internal sealed class BinaryCallObserver
{
    private readonly Func<BinaryRequestContext, ValueTask> _enter;
    private readonly Action? _exit;

    internal BinaryCallObserver(Action<BinaryRequestContext> observe)
    {
        ArgumentNullException.ThrowIfNull(observe);
        _enter = context =>
        {
            observe(context);
            return ValueTask.CompletedTask;
        };
    }

    internal BinaryCallObserver(Func<BinaryRequestContext, ValueTask> enter, Action exit)
    {
        _enter = enter;
        _exit = exit;
    }

    internal ValueTask EnterAsync(BinaryRequestContext context) => _enter(context);

    internal void Exit() => _exit?.Invoke();
}
