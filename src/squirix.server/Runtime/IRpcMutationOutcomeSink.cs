using System;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Squirix.Server.Runtime;

/// <summary>
/// Receiver of the outcome of a durable mutation that is known before the mutation is applied, so the outcome frame can follow the mutation
/// frame in the same journal flush. It is the active idempotency scope of an RPC.
/// </summary>
internal interface IRpcMutationOutcomeSink
{
    /// <summary>
    /// Appends the outcome frame of the running operation from the result its mutation will have, when a projection registered for the result
    /// type turns it into the response of the RPC; otherwise it appends nothing and the outcome is recorded after the apply as usual.
    /// </summary>
    /// <typeparam name="TResult">Result type of the durable mutation.</typeparam>
    /// <param name="predicted">The result the mutation will have once applied.</param>
    /// <returns>A task that completes once the outcome frame is on the journal ring, or was not appended.</returns>
    /// <remarks>
    /// A failure after the frame reached the ring is thrown: the outcome of the operation is then unknown. A failure before it is not: the
    /// operation continues on the unfused path, with no outcome frame appended.
    /// </remarks>
    ValueTask AppendPredictedOutcomeAsync<TResult>(TResult predicted);

    /// <summary>Records the appended outcome as completed; called right after the mutation was applied, never when the apply failed.</summary>
    void PromoteAfterApply();

    /// <summary>Registers how the handler of the running RPC turns the result of its mutation into the response of the RPC.</summary>
    /// <typeparam name="TResult">Result type of the durable mutation.</typeparam>
    /// <param name="projection">Builds the response from the predicted result.</param>
    void RegisterProjection<TResult>(Func<TResult, IMessage> projection);
}
