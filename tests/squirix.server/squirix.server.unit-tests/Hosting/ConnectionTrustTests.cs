using System;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies conservative OIDC trust history without token revalidation on requests.</summary>
[Immutable]
public sealed class ConnectionTrustTests
{
    /// <summary>Unknown internal LKG entries never receive an inferred token-length trust window.</summary>
    [Test]
    public async Task UnobservedHistoricalKeyIsRejected()
    {
        var options = CreateOptions(out _);
        var key = new JwtVerificationKey(new byte[32]);
        var token = new TrustEvidence("issuer", key);
        var expiry = DateTime.MaxValue;
        var accepted = new ConnectionTrustHistory().TryAuthorize(options, new JwtMetadata(), token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        _ = await Assert.That(accepted).IsFalse();
    }

    /// <summary>A recorded current validation caps historical trust even if the manager lifetime later grows.</summary>
    /// <param name="issuerMode">Selects metadata, ValidIssuer, or ValidIssuers trust.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task HistoricalLifetimeCannotBeExtended(int issuerMode)
    {
        var options = CreateOptions(out var manager);
        ConfigureIssuer(options, issuerMode);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        var key = new JwtVerificationKey(new byte[32]);
        var configuration = new JwtMetadata { Issuer = issuerMode == 0 ? "issuer" : "metadata" };
        configuration.SigningKeys.Add(key);
        var token = new TrustEvidence("issuer", key);
        var started = DateTime.UnixEpoch;
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        var current = history.TryAuthorize(options, configuration, token, started, TimeSpan.MaxValue, ref expiry, out _);
        manager.LastKnownGoodLifetime = TimeSpan.FromHours(1);
        expiry = DateTime.MaxValue;
        var historical = history.TryAuthorize(options, new JwtMetadata(), token, started.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out var recorded);
        _ = await Assert.That(current).IsTrue();
        _ = await Assert.That(historical).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(started.AddMinutes(1));
        _ = await Assert.That(recorded).IsEqualTo(started);
        expiry = DateTime.MaxValue;
        _ = await Assert.That(history.TryAuthorize(options, new JwtMetadata(), token, started.AddMinutes(1), TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
    }

    /// <summary>Reused keys retain historical issuer bounds unless the issuer is explicitly trusted as current.</summary>
    /// <param name="explicitIssuer">Whether the original issuer remains explicitly trusted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReusedKeyUsesBoundedIssuerTrust(bool explicitIssuer)
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        if (explicitIssuer)
            options.Issuer = "previous";
        var key = new JwtVerificationKey(new byte[32]);
        var original = new JwtMetadata { Issuer = "previous" };
        original.SigningKeys.Add(key);
        var replacement = new JwtMetadata { Issuer = "replacement" };
        replacement.SigningKeys.Add(key);
        var token = new TrustEvidence("previous", key);
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        var observed = history.TryAuthorize(options, original, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        var accepted = history.TryAuthorize(options, replacement, token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out var historical);
        _ = await Assert.That(observed).IsTrue();
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(historical).IsEqualTo(explicitIssuer ? null : DateTime.UnixEpoch);
        _ = await Assert.That(expiry).IsEqualTo(explicitIssuer ? DateTime.MaxValue : DateTime.UnixEpoch.AddMinutes(1));
    }

    /// <summary>Explicit issuer trust combines with exact metadata keys without trusting a matching key identifier.</summary>
    /// <param name="issuerMode">Selects absent, ValidIssuer, or ValidIssuers trust.</param>
    /// <param name="sameKey">Whether the token used the exact configured key object.</param>
    /// <param name="expected">Whether current trust can be proven.</param>
    [Test]
    [Arguments(1, true, true)]
    [Arguments(2, true, true)]
    [Arguments(0, true, false)]
    [Arguments(1, false, false)]
    public async Task CurrentKeyUsesConfiguredIssuer(int issuerMode, bool sameKey, bool expected)
    {
        var options = CreateOptions(out _);
        ConfigureIssuer(options, issuerMode);
        var key = new JwtVerificationKey(new byte[32]) { KeyId = "same" };
        var configuration = new JwtMetadata { Issuer = "metadata" };
        configuration.SigningKeys.Add(key);
        var tokenKey = sameKey ? key : new JwtVerificationKey(new byte[32]) { KeyId = "same" };
        var token = new TrustEvidence("issuer", tokenKey);
        var expiry = DateTime.MaxValue;
        var accepted = new ConnectionTrustHistory().TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out var historical);
        _ = await Assert.That(accepted).IsEqualTo(expected);
        _ = await Assert.That(historical).IsNull();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.MaxValue);
    }

    /// <summary>Disabling LKG immediately prevents historical bindings.</summary>
    [Test]
    public async Task DisabledHistoricalTrustIsRejected()
    {
        var options = CreateOptions(out _);
        var key = new JwtVerificationKey(new byte[32]);
        var configuration = new JwtMetadata { Issuer = "issuer" };
        configuration.SigningKeys.Add(key);
        var token = new TrustEvidence("issuer", key);
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        _ = history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        options.Metadata = new JwtMetadataSource("https://issuer/.well-known/openid-configuration", true) { UseLastKnownGoodConfiguration = false };
        _ = await Assert.That(history.TryAuthorize(options, new JwtMetadata(), token, DateTime.UnixEpoch.AddSeconds(1), TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
    }

    /// <summary>A reduced LKG lifetime immediately narrows already observed trust.</summary>
    [Test]
    public async Task HistoricalLifetimeShrinks()
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromHours(1);
        var key = new JwtVerificationKey(new byte[32]);
        var configuration = new JwtMetadata { Issuer = "issuer" };
        configuration.SigningKeys.Add(key);
        var token = new TrustEvidence("issuer", key);
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        _ = history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        var accepted = history.TryAuthorize(options, new JwtMetadata(), token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out _);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.UnixEpoch.AddMinutes(1));
    }

    /// <summary>Equivalent key material never borrows the validity proof of another configuration object.</summary>
    [Test]
    public async Task DistinctKeyCannotBorrowCurrentTrust()
    {
        var options = CreateOptions(out _);
        var configuration = new JwtMetadata { Issuer = "issuer" };
        configuration.SigningKeys.Add(new JwtVerificationKey(new byte[32]));
        var token = new TrustEvidence("issuer", new JwtVerificationKey(new byte[32]));
        var expiry = DateTime.MaxValue;
        _ = await Assert.That(new ConnectionTrustHistory().TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
    }

    /// <summary>A lifetime increase during validation cannot exceed the pre-validation trust budget.</summary>
    [Test]
    public async Task ValidationLifetimeIncreaseIsBounded()
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromHours(1);
        var key = new JwtVerificationKey(new byte[32]);
        var configuration = new JwtMetadata { Issuer = "issuer" };
        configuration.SigningKeys.Add(key);
        var token = new TrustEvidence("issuer", key);
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        _ = history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.FromMinutes(1), ref expiry, out _);
        var accepted = history.TryAuthorize(options, new JwtMetadata(), token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.FromHours(1), ref expiry, out _);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.UnixEpoch.AddMinutes(1));
    }

    /// <summary>An explicit static key can use the current metadata issuer without borrowing historical issuer trust.</summary>
    /// <param name="explicitIssuer">Whether the original issuer also has explicit options trust.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StaticKeyTracksMetadataIssuer(bool explicitIssuer)
    {
        var options = CreateOptions(out _);
        var key = new JwtVerificationKey(new byte[32]);
        options.SigningKey = key;
        if (explicitIssuer)
            options.Issuer = "metadata";
        var original = new JwtMetadata { Issuer = "metadata" };
        original.SigningKeys.Add(new JwtVerificationKey(new byte[32]));
        var replacement = new JwtMetadata { Issuer = "replacement" };
        var token = new TrustEvidence("metadata", key);
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        var accepted = history.TryAuthorize(options, original, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out var historical);
        var changed = history.TryAuthorize(options, replacement, token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out var changedHistory);
        var unknown = new TrustEvidence("untrusted", key);
        var unknownAccepted = history.TryAuthorize(options, original, unknown, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(historical).IsNull();
        _ = await Assert.That(changed).IsEqualTo(explicitIssuer);
        _ = await Assert.That(changedHistory).IsNull();
        _ = await Assert.That(unknownAccepted).IsFalse();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.MaxValue);
    }

    /// <summary>Overlapping proofs select the latest usable bound and its paired start independently of insertion order.</summary>
    /// <param name="newestFirst">Whether the newer proof was inserted first.</param>
    /// <param name="tokenExpirySeconds">The token's original expiry offset.</param>
    /// <param name="lifetimeSeconds">The current LKG lifetime after recording both proofs.</param>
    /// <param name="nowSeconds">The historical validation time offset.</param>
    /// <param name="expectedExpirySeconds">The selected expiry offset.</param>
    [Test]
    [Arguments(false, 120, 60, 70, 90)]
    [Arguments(true, 120, 60, 70, 90)]
    [Arguments(false, 80, 60, 70, 80)]
    [Arguments(false, 120, 45, 70, 75)]
    [Arguments(false, 120, 60, 30, 90)]
    public async Task HistoryChoosesLongestLiveProof(bool newestFirst, int tokenExpirySeconds, int lifetimeSeconds, int nowSeconds, int expectedExpirySeconds)
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromSeconds(60);
        options.Issuer = "issuer";
        var key = new JwtVerificationKey(new byte[32]);
        var history = new ConnectionTrustHistory();
        _ = await Assert.That(ObserveSnapshot(history, options, key, newestFirst ? "B" : "A", newestFirst ? 30 : 0)).IsTrue();
        _ = await Assert.That(ObserveSnapshot(history, options, key, newestFirst ? "A" : "B", newestFirst ? 0 : 30)).IsTrue();
        manager.LastKnownGoodLifetime = TimeSpan.FromSeconds(lifetimeSeconds);
        var token = new TrustEvidence("issuer", key);
        var expiry = DateTime.UnixEpoch.AddSeconds(tokenExpirySeconds);
        var accepted = history.TryAuthorize(options, new JwtMetadata { Issuer = "C" }, token, DateTime.UnixEpoch.AddSeconds(nowSeconds), TimeSpan.MaxValue, ref expiry, out var started);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.UnixEpoch.AddSeconds(expectedExpirySeconds));
        _ = await Assert.That(started).IsEqualTo(DateTime.UnixEpoch.AddSeconds(30));
    }

    /// <summary>Expired proofs leave the caller's original token expiry untouched and clear the selected start.</summary>
    /// <param name="tokenExpirySeconds">The original token expiry offset.</param>
    /// <param name="lifetimeSeconds">The current LKG lifetime.</param>
    /// <param name="nowSeconds">The historical validation time offset.</param>
    [Test]
    [Arguments(120, 60, 90)]
    [Arguments(70, 60, 70)]
    [Arguments(120, 20, 70)]
    public async Task ExpiredHistoryLeavesBoundsUntouched(int tokenExpirySeconds, int lifetimeSeconds, int nowSeconds)
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromSeconds(60);
        options.Issuer = "issuer";
        var key = new JwtVerificationKey(new byte[32]);
        var history = new ConnectionTrustHistory();
        _ = await Assert.That(ObserveSnapshot(history, options, key, "A", 0)).IsTrue();
        _ = await Assert.That(ObserveSnapshot(history, options, key, "B", 30)).IsTrue();
        manager.LastKnownGoodLifetime = TimeSpan.FromSeconds(lifetimeSeconds);
        var token = new TrustEvidence("issuer", key);
        var original = DateTime.UnixEpoch.AddSeconds(tokenExpirySeconds);
        var expiry = original;
        var accepted = history.TryAuthorize(options, new JwtMetadata { Issuer = "C" }, token, DateTime.UnixEpoch.AddSeconds(nowSeconds), TimeSpan.MaxValue, ref expiry, out var started);
        _ = await Assert.That(accepted).IsFalse();
        _ = await Assert.That(expiry).IsEqualTo(original);
        _ = await Assert.That(started).IsNull();
    }

    /// <summary>Other options, key objects, and issuers cannot borrow a later proof or mutate token bounds.</summary>
    /// <param name="mismatch">The unmatched proof dimension.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task UnmatchedHistoryLeavesBoundsUntouched(int mismatch)
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromSeconds(60);
        options.Issuer = "issuer";
        var key = new JwtVerificationKey(new byte[32]) { KeyId = "same" };
        var history = new ConnectionTrustHistory();
        _ = await Assert.That(ObserveSnapshot(history, options, key, "A", 0)).IsTrue();
        _ = await Assert.That(ObserveSnapshot(history, options, key, "B", 30)).IsTrue();
        var actualOptions = mismatch == 0 ? CreateOptions(out _) : options;
        var actualKey = mismatch == 1 ? new JwtVerificationKey(new byte[32]) { KeyId = "same" } : key;
        var token = new TrustEvidence(mismatch == 2 ? "other" : "issuer", actualKey);
        var original = DateTime.UnixEpoch.AddSeconds(120);
        var expiry = original;
        var accepted = history.TryAuthorize(actualOptions, new JwtMetadata { Issuer = "C" }, token, DateTime.UnixEpoch.AddSeconds(70), TimeSpan.MaxValue, ref expiry, out var started);
        _ = await Assert.That(accepted).IsFalse();
        _ = await Assert.That(expiry).IsEqualTo(original);
        _ = await Assert.That(started).IsNull();
    }

    private static bool ObserveSnapshot(ConnectionTrustHistory history, JwtValidationOptions options, JwtVerificationKey key, string issuer, int startSeconds)
    {
        var configuration = new JwtMetadata { Issuer = issuer };
        configuration.SigningKeys.Add(key);
        var token = new TrustEvidence("issuer", key);
        var expiry = DateTime.MaxValue;
        return history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch.AddSeconds(startSeconds), TimeSpan.MaxValue, ref expiry, out _);
    }

    private static void ConfigureIssuer(JwtValidationOptions parameters, int issuerMode)
    {
        if (issuerMode == 1)
            parameters.Issuer = "issuer";
        if (issuerMode == 2)
            parameters.AdditionalIssuers = ["issuer"];
    }

    private static JwtValidationOptions CreateOptions(out JwtMetadataSource manager)
    {
        manager = new JwtMetadataSource("https://issuer/.well-known/openid-configuration", true);
        return new JwtValidationOptions { Metadata = manager };
    }
}
