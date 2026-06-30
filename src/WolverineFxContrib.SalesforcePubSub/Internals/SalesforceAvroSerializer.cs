using System.Runtime.CompilerServices;
using SolTechnology.Avro;
using Wolverine.Runtime.Serialization;
using Wolverine.SalesforcePubSub.Events;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Decodes a Salesforce Avro event body to its .NET message type inside Wolverine's serializer pipeline.
/// The schema is resolved by id from <see cref="CachingSchemaRepository"/>, which the listener pre-fetches
/// (async, in the consume loop) before handing the envelope off — so the decode here is <b>synchronous</b>,
/// and a token/auth failure during schema fetch surfaces to the reconnect loop rather than this pipeline
/// path. Listen-only: the outbound/write members are unsupported.
/// </summary>
internal sealed class SalesforceAvroSerializer : IMessageSerializer
{
    public const string SalesforceAvroContentType = "application/x-salesforce-avro";
    public const string SchemaIdHeader = "sfdc-schema-id";

    private readonly CachingSchemaRepository _schemaRepository;

    public SalesforceAvroSerializer(CachingSchemaRepository schemaRepository)
        => _schemaRepository = schemaRepository ?? throw new ArgumentNullException(nameof(schemaRepository));

    public string ContentType => SalesforceAvroContentType;

    public object ReadFromData(Type messageType, Envelope envelope)
    {
        if (!envelope.Headers.TryGetValue(SchemaIdHeader, out var schemaId) || string.IsNullOrEmpty(schemaId))
            throw new InvalidOperationException($"Envelope {envelope.Id} is missing the '{SchemaIdHeader}' header.");

        if (!_schemaRepository.TryGetCachedSchema(schemaId, out var schema))
            throw new InvalidOperationException(
                $"Avro schema '{schemaId}' is not cached for envelope {envelope.Id}; the listener should have fetched it before dispatch.");

        // Wolverine's pipeline guarantees Data is present before calling ReadFromData.
        var message = AvroConvert.DeserializeHeadless(envelope.Data!, schema.SchemaJson, messageType)
            ?? throw new InvalidOperationException($"Avro deserialization produced null for {messageType}.");

        // The listener no longer holds the decoded object, so stamp transport metadata here.
        if (message is PubSubEvent pubSubEvent)
            pubSubEvent.ReplayId = envelope.Offset;
        if (message is PlatformEvent platformEvent)
            envelope.SentAt = DateTimeOffset.FromUnixTimeMilliseconds(platformEvent.CreatedDate);

        return message;
    }

    public byte[] Write(Envelope envelope) => throw NotSupported();
    public object ReadFromData(byte[] data) => throw NotSupported();
    public byte[] WriteMessage(object message) => throw NotSupported();

    private static NotSupportedException NotSupported([CallerMemberName] string? member = null)
        => new($"{nameof(SalesforceAvroSerializer)}.{member} is not supported; this transport is listen-only.");
}
