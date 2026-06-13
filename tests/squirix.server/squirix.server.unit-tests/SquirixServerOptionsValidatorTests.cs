using System;
using System.Text.Json;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Covers misuse prevention for the public server-hosting options.
/// </summary>
public sealed class SquirixServerOptionsValidatorTests
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// Ensures the public defaults are valid and use the first reserved standalone port.
    /// </summary>
    [Fact]
    public void DefaultsAreValid()
    {
        var options = new SquirixServerOptions();

        SquirixServerOptionsValidator.Validate(options);

        Assert.Equal(new Uri("https://localhost:5001"), options.Url);
    }

    /// <summary>
    /// Ensures duplicate peer node identifiers are rejected.
    /// </summary>
    [Fact]
    public void DuplicatePeerNodeIdIsRejected()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Url = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5001") },
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5002") },
            ],
        };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures duplicate peer URLs are rejected.
    /// </summary>
    [Fact]
    public void DuplicatePeerUrlIsRejected()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Url = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5001") },
                new SquirixServerPeerOptions { NodeId = "node-b", Url = new Uri("https://localhost:5001") },
            ],
        };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures blank and excessively long identifiers are rejected.
    /// </summary>
    /// <param name="nodeId">Invalid node identifier.</param>
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void InvalidNodeIdIsRejected(string nodeId)
    {
        var options = new SquirixServerOptions { NodeId = nodeId };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures JSON deserialization populates explicit peers for settings validation.
    /// </summary>
    [Fact]
    public void JsonDeserializationPopulatesPeers()
    {
        const string json = """
                            {
                              "ClusterId": "c",
                              "NodeId": "node-a",
                              "Url": "https://localhost:5001",
                              "VirtualNodes": 128,
                              "Peers": [
                                { "NodeId": "node-b", "Url": "https://localhost:5002" }
                              ]
                            }
                            """;

        var options = JsonSerializer.Deserialize<SquirixServerOptions>(json, JsonOptions) ?? throw new InvalidOperationException("Deserialization failed.");

        _ = Assert.Single(options.Peers);
        Assert.Equal("node-b", options.Peers[0].NodeId);
        var ex = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
        Assert.Contains("local NodeId", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures updating the local peer URL to match the node URL satisfies validation.
    /// </summary>
    [Fact]
    public void LocalPeerUrlAlignedWithNodeUrlIsAccepted()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Url = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5001") },
                new SquirixServerPeerOptions { NodeId = "node-b", Url = new Uri("https://localhost:5002") },
            ],
        };
        options.Peers[0].Url = options.Url;

        SquirixServerOptionsValidator.Validate(options);
    }

    /// <summary>
    /// Ensures the local peer URL matches the node URL.
    /// </summary>
    [Fact]
    public void LocalPeerUrlMismatchIsRejected()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Url = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5999") },
                new SquirixServerPeerOptions { NodeId = "node-b", Url = new Uri("https://localhost:5002") },
            ],
        };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures node identifiers have a bounded length.
    /// </summary>
    [Fact]
    public void LongNodeIdIsRejected()
    {
        var options = new SquirixServerOptions { NodeId = new string('n', 129) };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures explicitly configured peers contain the local node.
    /// </summary>
    [Fact]
    public void PeersWithoutLocalNodeAreRejected()
    {
        var options = new SquirixServerOptions
        {
            Peers = [new SquirixServerPeerOptions { NodeId = "other", Url = new Uri("https://localhost:5002") }],
        };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures peer endpoint URIs cannot carry HTTP resource paths.
    /// </summary>
    [Fact]
    public void PeerUrlWithPathIsRejected()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Url = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5001") },
                new SquirixServerPeerOptions { NodeId = "node-b", Url = new Uri("https://localhost:5002/cache") },
            ],
        };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures endpoint URIs cannot carry HTTP resource paths.
    /// </summary>
    [Fact]
    public void UrlWithPathIsRejected()
    {
        var options = new SquirixServerOptions { Url = new Uri("https://localhost:5001/cache") };

        _ = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
    }

    /// <summary>
    /// Ensures a multi-peer cluster with a matching local peer is accepted.
    /// </summary>
    [Fact]
    public void ValidMultiPeerClusterIsAccepted()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Url = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Url = new Uri("https://localhost:5001") },
                new SquirixServerPeerOptions { NodeId = "node-b", Url = new Uri("https://localhost:5002") },
            ],
        };

        SquirixServerOptionsValidator.Validate(options);
    }
}
