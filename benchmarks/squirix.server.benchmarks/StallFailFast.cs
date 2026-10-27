namespace Squirix.Server.Benchmarks;

/// <summary>What an ideal fail-fast refusal after a threshold would have given on the observed timeline.</summary>
/// <param name="ThresholdSeconds">The threshold N.</param>
/// <param name="Refused">Requests issued after the threshold, which would have been refused immediately.</param>
/// <param name="Collateral">Refused requests that actually committed within their deadline.</param>
/// <param name="SavedClientSeconds">The wait of the refused requests the clients would have been spared.</param>
/// <param name="FailFastFailureP99Ms">The p99 wait until a failure with fail-fast.</param>
/// <param name="ObservedFailureP99Ms">The observed p99 wait until a failure.</param>
/// <param name="UnknownAvoided">Refused requests that were observed as a client timeout with the frame already queued, an unknown outcome the refusal would have turned into a definite one.</param>
/// <param name="PeakInflight">The peak requests the server runs with fail-fast, over the whole run.</param>
/// <param name="ObservedPeakInflight">The peak requests the server ran without fail-fast, computed from the same request records.</param>
internal sealed record StallFailFast(
    double ThresholdSeconds,
    int Refused,
    int Collateral,
    double SavedClientSeconds,
    double FailFastFailureP99Ms,
    double ObservedFailureP99Ms,
    int UnknownAvoided,
    int PeakInflight,
    int ObservedPeakInflight);
