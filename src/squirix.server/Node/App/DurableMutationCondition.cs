using Squirix.Server.Attributes;

namespace Squirix.Server.Node.App;

[Immutable]
internal readonly record struct DurableMutationCondition<TResult>
{
    private DurableMutationCondition(bool shouldApply, TResult? skipResult, bool hasPrediction = false, TResult? predicted = default)
    {
        ShouldApply = shouldApply;
        SkipResult = skipResult;
        HasPrediction = hasPrediction;
        Predicted = predicted;
    }

    /// <summary>Gets a value indicating whether the result of the mutation is already known, so <see cref="Predicted" /> holds it.</summary>
    internal bool HasPrediction { get; }

    /// <summary>Gets the result the mutation will have once applied, when <see cref="HasPrediction" /> is true.</summary>
    internal TResult? Predicted { get; }

    internal bool ShouldApply { get; }

    /// <summary>Gets the result returned when <see cref="ShouldApply" /> is false.</summary>
    internal TResult? SkipResult { get; }

    internal static DurableMutationCondition<TResult> Apply() => new(true, default);

    /// <summary>Creates a condition that applies the mutation and states its result in advance.</summary>
    /// <param name="predicted">The result the mutation will have: what the precondition decided under the key lock that holds until the apply.</param>
    /// <returns>An apply condition that carries the predicted result.</returns>
    internal static DurableMutationCondition<TResult> Apply(TResult predicted) => new(true, default, true, predicted);

    internal static DurableMutationCondition<TResult> Skip(TResult result) => new(false, result);
}
