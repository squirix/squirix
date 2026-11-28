using System.Text.Json.Serialization;

namespace Squirix.Server;

/// <summary>Settings read from the standalone host's RESP configuration section.</summary>
internal sealed class RespFileSettings
{
    [JsonInclude]
    internal bool Enabled { get; set; } = true;

    [JsonInclude]
    internal string BindAddress { get; set; } = RespOptions.DefaultBindAddress;

    [JsonInclude]
    internal int Port { get; set; } = RespOptions.DefaultPort;

    internal RespOptions ToOptions() => new()
    {
        Enabled = Enabled,
        BindAddress = BindAddress,
        Port = Port,
    };
}
