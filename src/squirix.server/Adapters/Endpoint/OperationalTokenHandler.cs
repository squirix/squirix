using System;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Uses the canonical JWT policy exclusively for operational HTTP requests.</summary>
internal sealed class OperationalTokenHandler : JwtBearerHandler
{
    public OperationalTokenHandler(IOptionsMonitor<JwtBearerOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : base(new CanonicalOptions(options), logger, encoder)
    {
    }

    private sealed class CanonicalOptions : IOptionsMonitor<JwtBearerOptions>
    {
        private readonly IOptionsMonitor<JwtBearerOptions> _options;

        public CanonicalOptions(IOptionsMonitor<JwtBearerOptions> options)
        {
            _options = options;
        }

        public JwtBearerOptions CurrentValue => Get(null);

        public JwtBearerOptions Get(string? name) => _options.Get(JwtBearerDefaults.AuthenticationScheme);

        public IDisposable? OnChange(Action<JwtBearerOptions, string?> listener) => _options.OnChange(listener);
    }
}
