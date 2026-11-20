using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Preserves virtual authorization semantics while enforcing independent identity snapshots.</summary>
[Immutable]
public sealed class PrincipalCloneTests
{
    /// <summary>A typed custom clone retains denied roles, claim overrides, and independent custom state.</summary>
    [Test]
    public async Task CustomPolicyAndStateArePreserved()
    {
        var source = new PolicyPrincipal("original");
        var identity = new ClaimsIdentity([new Claim(ClaimTypes.Role, "admin"), new Claim("permission", "grant"), new Claim("seed", "original")], "Bearer");
        identity.FindFirst("seed")!.Properties["state"] = "original";
        source.AddIdentity(identity);
        var connection = CreateConnection(source);
        source.Marker = "source";
        identity.FindFirst("seed")!.Properties["state"] = "source";
        var first = Copy(connection);
        _ = await Assert.That(first!.IsInRole("admin")).IsFalse();
        _ = await Assert.That(first.HasClaim("permission", "grant")).IsFalse();
        _ = await Assert.That(first.FindFirst("permission")!.Value).IsEqualTo("grant");
        _ = await Assert.That(first.FindFirst("seed")!.Properties["state"]).IsEqualTo("original");
        if (first is PolicyPrincipal custom)
            custom.Marker = "request";
        first.FindFirst("seed")!.Properties["state"] = "request";
        var second = Copy(connection);
        _ = await Assert.That(second is PolicyPrincipal secondPolicy && string.Equals(secondPolicy.Marker, "original", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(second!.IsInRole("admin")).IsFalse();
        _ = await Assert.That(second.HasClaim("permission", "grant")).IsFalse();
        _ = await Assert.That(second.FindFirst("seed")!.Properties["state"]).IsEqualTo("original");
    }

    /// <summary>Self, base-type, and source-sharing principal clones cannot become connection identities.</summary>
    /// <param name="mode">The malformed clone contract.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task MalformedPrincipalCloneIsRejected(int mode) =>
        _ = await Assert.That(CreateConnection(new UnsupportedPrincipal(mode))).IsNull();

    /// <summary>An identity clone cannot share the original or erase an authentication override.</summary>
    /// <param name="mode">The malformed clone contract.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task MalformedIdentityCloneIsRejected(int mode)
    {
        var identity = new UnsupportedIdentity(mode);
        _ = await Assert.That(identity.IsAuthenticated).IsFalse();
        _ = await Assert.That(CreateConnection(new ClaimsPrincipal(identity))).IsNull();
    }

    /// <summary>A derived principal cannot hide a source-sharing or type-erasing identity clone.</summary>
    /// <param name="mode">The malformed identity clone contract.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task CustomPrincipalCannotEraseIdentity(int mode)
    {
        var source = new PolicyPrincipal("original");
        source.AddIdentity(new UnsupportedIdentity(mode));
        _ = await Assert.That(CreateConnection(source)).IsNull();
    }

    /// <summary>A correctly typed identity clone retains its authentication override and mutable state ownership.</summary>
    [Test]
    public async Task TypedIdentityPolicyIsPreserved()
    {
        var identity = new PolicyIdentity();
        var connection = CreateConnection(new ClaimsPrincipal(identity));
        identity.State = "source";
        var first = Copy(connection);
        _ = await Assert.That(first!.Identity!.IsAuthenticated).IsFalse();
        _ = await Assert.That(first.Identity is PolicyIdentity firstPolicy && string.Equals(firstPolicy.State, "original", StringComparison.Ordinal)).IsTrue();
        if (first.Identity is PolicyIdentity custom)
            custom.State = "request";
        _ = await Assert.That(Copy(connection)?.Identity is PolicyIdentity laterIdentity && string.Equals(laterIdentity.State, "original", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>The default IdentityModel identity preserves its exact type and case-sensitive claims.</summary>
    [Test]
    public async Task IdentityModelClonePreservesType()
    {
        var identity = new CaseSensitiveClaimsIdentity([new Claim("Permission", "grant")], "Bearer");
        var connection = CreateConnection(new ClaimsPrincipal(identity));
        var copy = Copy(connection);
        _ = await Assert.That(copy!.Identity is CaseSensitiveClaimsIdentity).IsTrue();
        _ = await Assert.That(copy.HasClaim("Permission", "grant")).IsTrue();
        _ = await Assert.That(copy.HasClaim("permission", "grant")).IsFalse();
    }

    /// <summary>A malformed request-time clone is rejected after a valid owned snapshot was accepted.</summary>
    [Test]
    public async Task LaterMalformedCloneIsRejected()
    {
        var source = new ChangingPrincipal(true);
        var connection = CreateConnection(source);
        _ = await Assert.That(connection).IsNotNull();
        _ = await Assert.That(connection!.ClonePrincipal()).IsNull();
    }

    private static AuthenticatedConnection? CreateConnection(ClaimsPrincipal principal) =>
        AuthenticatedConnection.Create(principal, (DateTime.UnixEpoch, DateTime.MaxValue), new JwtValidationOptions(), null, null, static () => { });

    private static ClaimsPrincipal? Copy(AuthenticatedConnection? connection) => connection?.ClonePrincipal();

    private sealed class PolicyPrincipal : ClaimsPrincipal
    {
        internal PolicyPrincipal(string marker)
        {
            Marker = marker;
        }

        internal string Marker { get; set; }

        public override bool IsInRole(string role) => false;

        public override bool HasClaim(string type, string value) => false;

        public override ClaimsPrincipal Clone()
        {
            var clone = new PolicyPrincipal(Marker);
            foreach (var identity in Identities)
                clone.AddIdentity(identity.Clone());
            return clone;
        }
    }

    private sealed class UnsupportedPrincipal : ClaimsPrincipal
    {
        private readonly int _mode;

        internal UnsupportedPrincipal(int mode)
            : base(new ClaimsIdentity([new Claim(ClaimTypes.Role, "admin")], "Bearer"))
        {
            _mode = mode;
        }

        private UnsupportedPrincipal(int mode, IEnumerable<ClaimsIdentity> identities)
            : base(identities)
        {
            _mode = mode;
        }

        public override ClaimsPrincipal Clone() => _mode switch
        {
            0 => this,
            2 => new UnsupportedPrincipal(_mode, Identities),
            _ => base.Clone(),
        };
    }

    private sealed class UnsupportedIdentity : ClaimsIdentity
    {
        private readonly int _mode;

        internal UnsupportedIdentity(int mode)
            : base("Bearer")
        {
            _mode = mode;
        }

        public override bool IsAuthenticated => false;

        public override ClaimsIdentity Clone() => _mode switch
        {
            0 => this,
            _ => new ClaimsIdentity(this),
        };
    }

    private sealed class PolicyIdentity : ClaimsIdentity
    {
        internal PolicyIdentity()
            : base("Bearer")
        {
            State = "original";
        }

        private PolicyIdentity(PolicyIdentity source)
            : base(source, StringComparison.OrdinalIgnoreCase)
        {
            State = source.State;
        }

        public override bool IsAuthenticated => false;

        internal string State { get; set; }

        public override ClaimsIdentity Clone() => new PolicyIdentity(this);
    }

    private sealed class ChangingPrincipal : ClaimsPrincipal
    {
        private readonly bool _allowClone;

        internal ChangingPrincipal(bool allowClone)
            : base(new ClaimsIdentity("Bearer"))
        {
            _allowClone = allowClone;
        }

        public override ClaimsPrincipal Clone() => _allowClone ? new ChangingPrincipal(false) : this;
    }

}
