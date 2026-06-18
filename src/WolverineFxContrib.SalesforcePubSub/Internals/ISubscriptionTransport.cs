namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Abstracts the protocol-specific gRPC streaming behavior for a single connection attempt.
/// Implementations handle the differences between topic subscriptions and managed event subscriptions.
/// Each transport represents one gRPC duplex stream — create a new transport for each reconnect.
/// </summary>
internal interface ISubscriptionTransport : IDisposable
{
    /// <summary>
    /// Opens the gRPC duplex stream and sends the initial fetch request.
    /// </summary>
    Task ConnectAsync(CancellationToken ct);

    /// <summary>
    /// Reads responses from the stream, yielding a <see cref="ResponseMessageInfo"/> for each
    /// meaningful response. Protocol-specific noise (e.g., managed commit acknowledgments)
    /// is filtered internally and not yielded.
    /// </summary>
    IAsyncEnumerable<ResponseMessageInfo> ReadAsync(CancellationToken ct);

    /// <summary>
    /// Persists or commits the replay id after events have been processed. For topics this writes to
    /// <see cref="IReplayIdRepository"/>; for managed subscriptions this writes a commit request back
    /// to the stream.
    /// </summary>
    Task AcknowledgeAsync(ResponseMessageInfo response, CancellationToken ct);

    /// <summary>
    /// Sends a new fetch request when the pending request count reaches zero.
    /// </summary>
    Task RequestMoreAsync(CancellationToken ct);

    /// <summary>
    /// Called when the stream faults. Attempts to cleanly complete the request stream, retrieves the
    /// gRPC call status, and performs any protocol-specific error recovery (e.g., resetting replay id
    /// on validation failure for topics). Returns the gRPC status string for logging, or null.
    /// </summary>
    Task<string?> HandleErrorAsync(CancellationToken ct);
}
