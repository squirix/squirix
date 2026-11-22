using System;

namespace Squirix.Transport;

internal readonly record struct CallStatus(CacheTransportStatusCode Code, string Detail, Exception? Cause = null, bool PayloadTooLarge = false);
