using Wolverine.Configuration;
using Wolverine.Transports;

namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Wolverine transport for the Salesforce Pub/Sub gRPC API. Listen-only: it surfaces topic
/// subscriptions and managed event subscriptions (MES) as Wolverine listening endpoints.
/// </summary>
public sealed class SalesforcePubSubTransport : TransportBase<SalesforceEndpoint>
{
    public const string ProtocolName = "sfpubsub";
    private const string TransportName = "Salesforce Pub/Sub";

    private readonly List<SalesforceEndpoint> _endpoints = [];

    public SalesforcePubSubTransport() : base(ProtocolName, TransportName, [ProtocolName])
    {
    }

    /// <summary>
    /// The transport-level settings, owned by the transport instance (the Kafka pattern) so that a
    /// second <c>UseSalesforcePubSub</c> call composes onto the same configuration instead of silently
    /// mutating an instance the container never sees.
    /// </summary>
    internal SubscriberComponentsSettings Settings { get; } = new();

    /// <summary>Guards the one-time DI wiring (the gRPC client registration is not idempotent).</summary>
    internal bool ServicesRegistered { get; set; }

    /// <summary>Finds an existing endpoint for the resource or creates and registers a new one.</summary>
    internal SalesforceEndpoint EndpointForResource(SalesforceResourceKind kind, string resource)
    {
        var uri = SalesforceEndpoint.BuildUri(kind, resource);
        var existing = _endpoints.FirstOrDefault(e => e.Uri == uri);
        if (existing is not null)
            return existing;

        var endpoint = new SalesforceEndpoint(this, kind, resource, EndpointRole.Application);
        _endpoints.Add(endpoint);
        return endpoint;
    }

    protected override IEnumerable<SalesforceEndpoint> endpoints() => _endpoints;

    protected override SalesforceEndpoint findEndpointByUri(Uri uri)
        => _endpoints.FirstOrDefault(e => e.Uri == uri)
           ?? throw new ArgumentOutOfRangeException(nameof(uri), $"Unknown Salesforce Pub/Sub endpoint: {uri}");

    /// <summary>
    /// Listen-only: this transport cannot send, so it can never serve as a request/reply target. The base
    /// default returns one of our listeners, which would route replies into a sender we don't have
    /// (<see cref="SalesforceEndpoint"/> throws from CreateSender). Return null instead.
    /// </summary>
    public override Endpoint? ReplyEndpoint() => null;
}
