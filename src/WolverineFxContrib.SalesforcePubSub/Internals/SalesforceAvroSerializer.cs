using System.Runtime.CompilerServices;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SolTechnology.Avro;
using Wolverine.Runtime.Serialization;
using Wolverine.SalesforcePubSub.Events;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Decodes a Salesforce Avro event body to its .NET message type inside Wolverine's serializer pipeline.
/// On the hot path the schema is already cached: the listener pre-fetches it (async, in the consume loop)
/// before handing the envelope off, so the sync decode never touches the network and a token/auth failure
/// during schema fetch surfaces to the reconnect loop. The async path adds a fetch-on-miss by the
/// persisted schema-id header for envelopes replayed by durable restart-recovery, where no listener
/// pre-fetch ever ran (DECISIONS #17) — mirroring how Wolverine's Kafka SchemaRegistryAvroSerializer
/// resolves the persisted schema pointer wherever decoding runs. Listen-only: the outbound/write members
/// are unsupported.
/// </summary>
internal sealed class SalesforceAvroSerializer : IAsyncMessageSerializer
{
    public const string SalesforceAvroContentType = "application/x-salesforce-avro";
    public const string SchemaIdHeader = "sfdc-schema-id";

    /// <summary>
    /// The event's replay id, persisted as a header because <see cref="Envelope.Offset"/> is a runtime
    /// property the durable inbox does not round-trip — without it a recovered event would decode with
    /// ReplayId 0 (found live in the Durable restart-recovery test).
    /// </summary>
    public const string ReplayIdHeader = "sfdc-replay-id";

    private readonly CachingSchemaRepository _schemaRepository;
    private readonly CachingAuthenticationTokenProvider _tokenProvider;
    private readonly ILogger _logger;

    public SalesforceAvroSerializer(CachingSchemaRepository schemaRepository, CachingAuthenticationTokenProvider tokenProvider, ILogger? logger = null)
    {
        _schemaRepository = schemaRepository ?? throw new ArgumentNullException(nameof(schemaRepository));
        _tokenProvider = tokenProvider ?? throw new ArgumentNullException(nameof(tokenProvider));
        _logger = logger ?? NullLogger.Instance;
    }

    public string ContentType => SalesforceAvroContentType;

    public object ReadFromData(Type messageType, Envelope envelope)
    {
        var schemaId = RequireSchemaId(envelope);

        if (!_schemaRepository.TryGetCachedSchema(schemaId, out var schema))
            throw new InvalidOperationException(
                $"Avro schema '{schemaId}' is not cached for envelope {envelope.Id}; the listener should have fetched it before dispatch.");

        return Decode(schema.SchemaJson, messageType, envelope);
    }

    public async ValueTask<object?> ReadFromDataAsync(Type messageType, Envelope envelope)
    {
        var schemaId = RequireSchemaId(envelope);

        if (_schemaRepository.TryGetCachedSchema(schemaId, out var cached))
            return Decode(cached.SchemaJson, messageType, envelope);

        // Cache miss — durable restart-recovery in a fresh process (no listener pre-fetch ran for this
        // envelope). Fetch by the persisted schema id with the listener's auth contract: a token rejected
        // by Salesforce is invalidated and the fetch retried once with a fresh one, so a revoked-but-cached
        // token can never poison the whole recovery batch. A still-failing fetch propagates, and Wolverine
        // moves the envelope to the durable dead-letter queue — parked and replayable, not lost.
        _logger.LogDebug("Schema {SchemaId} is not cached for envelope {EnvelopeId} (recovery decode); fetching by the persisted id.",
            schemaId, envelope.Id);

        SalesforceGrpc.SchemaInfo schema;
        try
        {
            schema = await _schemaRepository.GetDeserializationInfoBySchemaIdAsync(schemaId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (RpcException ex) when (SalesforceAuthErrors.IsAuthRejection(ex))
        {
            _logger.LogInformation("Authentication failure fetching schema {SchemaId} during recovery decode; invalidating the cached token and retrying once.", schemaId);
            _tokenProvider.Invalidate();
            schema = await _schemaRepository.GetDeserializationInfoBySchemaIdAsync(schemaId, CancellationToken.None).ConfigureAwait(false);
        }

        return Decode(schema.SchemaJson, messageType, envelope);
    }

    private static string RequireSchemaId(Envelope envelope)
    {
        if (!envelope.Headers.TryGetValue(SchemaIdHeader, out var schemaId) || string.IsNullOrEmpty(schemaId))
            throw new InvalidOperationException($"Envelope {envelope.Id} is missing the '{SchemaIdHeader}' header.");
        return schemaId;
    }

    private static object Decode(string schemaJson, Type messageType, Envelope envelope)
    {
        // Wolverine's pipeline guarantees Data is present before calling ReadFromData.
        var message = AvroConvert.DeserializeHeadless(envelope.Data!, schemaJson, messageType)
            ?? throw new InvalidOperationException($"Avro deserialization produced null for {messageType}.");

        // The listener no longer holds the decoded object, so stamp transport metadata here. The replay id
        // comes from the persisted header (survives durable recovery); Offset is the live-path fallback.
        if (message is PubSubEvent pubSubEvent)
            pubSubEvent.ReplayId =
                envelope.Headers.TryGetValue(ReplayIdHeader, out var replayId) && long.TryParse(replayId, out var parsed)
                    ? parsed
                    : envelope.Offset;
        if (message is PlatformEvent platformEvent)
            envelope.SentAt = DateTimeOffset.FromUnixTimeMilliseconds(platformEvent.CreatedDate);

        return message;
    }

    public byte[] Write(Envelope envelope) => throw NotSupported();
    public ValueTask<byte[]> WriteAsync(Envelope envelope) => throw NotSupported();
    public object ReadFromData(byte[] data) => throw NotSupported();
    public byte[] WriteMessage(object message) => throw NotSupported();

    private static NotSupportedException NotSupported([CallerMemberName] string? member = null)
        => new($"{nameof(SalesforceAvroSerializer)}.{member} is not supported; this transport is listen-only.");
}
