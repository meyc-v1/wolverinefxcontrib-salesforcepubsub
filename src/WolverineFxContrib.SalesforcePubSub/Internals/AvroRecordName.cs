using System.Text.Json;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Extracts the top-level Avro record name from a Salesforce <c>schema_json</c>. For platform events
/// the record name is the full event API name including the <c>__e</c> suffix (e.g.
/// "CM_Test_Event_Two__e", namespace "com.sforce.eventbus" — verified live against the sandbox org), which is
/// exactly the <c>MapEvent</c> key.
/// </summary>
internal static class AvroRecordName
{
    public static string Parse(string schemaJson)
    {
        using var document = JsonDocument.Parse(schemaJson);

        if (document.RootElement.ValueKind == JsonValueKind.Object
            && document.RootElement.TryGetProperty("name", out var name)
            && name.ValueKind == JsonValueKind.String
            && name.GetString() is { Length: > 0 } value)
        {
            return value;
        }

        throw new InvalidOperationException("The Avro schema JSON has no top-level record name.");
    }
}
