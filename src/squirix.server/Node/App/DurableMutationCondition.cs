namespace Squirix.Server.Node.App;

internal readonly record struct DurableMutationCondition<TResult>(bool ShouldApply, TResult Result)
{
    public static DurableMutationCondition<TResult> Apply() => new(true, default!);
}
