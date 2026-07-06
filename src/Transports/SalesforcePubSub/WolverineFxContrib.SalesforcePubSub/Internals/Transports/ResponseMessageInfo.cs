using Google.Protobuf;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals.Transports;

/// <summary>
/// Protocol-agnostic projection of a fetch response — the common shape between a topic
/// <c>FetchResponse</c> and a managed <c>ManagedFetchResponse</c>.
/// </summary>
internal sealed class ResponseMessageInfo
{
    public ByteString LastReplayIdByteString { get; set; } = null!;
    public long LastReplayId { get; set; }
    public IReadOnlyList<ConsumerEvent> Events { get; set; } = [];
    public int PendingNumberRequested { get; set; }
}
