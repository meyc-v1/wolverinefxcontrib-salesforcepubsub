namespace TestHost.Settings;

/// <summary>
/// Drives the timed test publisher (revived from the original runner's RandomWriterService). Opt-in: the
/// host stays listen-only unless <see cref="Enabled"/> is set, so plain connect/keep-alive runs are quiet.
/// </summary>
public sealed class PublisherSettings
{
    /// <summary>Publish test events on a timer. Default off.</summary>
    public bool Enabled { get; set; }

    /// <summary>Seconds between publishes (original used 15s).</summary>
    public int IntervalSeconds { get; set; } = 15;

    /// <summary>
    /// Platform event sObject API names to publish to, round-robin (e.g. <c>CM_Test_Event_Two__e</c>).
    /// These are the underlying PEs — a topic and an MES both observe the same published event.
    /// </summary>
    public List<string> Events { get; set; } = [];
}
