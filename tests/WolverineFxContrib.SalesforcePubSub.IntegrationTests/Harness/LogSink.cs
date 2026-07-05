using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

/// <summary>
/// Captures a host's log lines so tests can react to listener conditions that never surface as
/// exceptions — the reconnect loop swallows everything. Primary use: detecting the MES
/// ALREADY_EXISTS slot-held condition to skip (not fail) the MES tests.
/// </summary>
public sealed class LogSink
{
    private readonly ConcurrentQueue<string> _lines = new();

    public void Add(string line) => _lines.Enqueue(line);

    public bool Any(Func<string, bool> match) => _lines.Any(match);

    /// <summary>The MES slot is exclusive per client; an unclean disconnect holds it ~15 minutes.</summary>
    public bool SawMesSlotHeld()
        => Any(l => l.Contains("AlreadyExists", StringComparison.OrdinalIgnoreCase)
                 || l.Contains("already.active", StringComparison.OrdinalIgnoreCase));
}

internal sealed class SinkLoggerProvider(LogSink sink) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new SinkLogger(sink);

    public void Dispose()
    {
    }

    private sealed class SinkLogger(LogSink sink) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
            => sink.Add(exception is null ? formatter(state, exception) : $"{formatter(state, exception)} | {exception}");
    }
}
