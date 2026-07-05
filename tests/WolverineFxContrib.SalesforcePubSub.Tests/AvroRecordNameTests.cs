using Wolverine.SalesforcePubSub.Internals.Schema;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The record-name parse that drives per-event type resolution: the top-level "name" of a Salesforce
/// platform-event schema is the full event API name including __e (verified live against a sandbox org).
/// </summary>
public class AvroRecordNameTests
{
    [Fact]
    public void Parses_the_real_salesforce_compact_schema_shape()
    {
        // The verbatim shape Salesforce returns (captured live, 2026-07-02).
        const string schema = """
            {"name":"CM_Test_Event_Two__e","namespace":"com.sforce.eventbus","type":"record",
             "fields":[{"name":"CreatedDate","type":"long","doc":"CreatedDate:DateTime"},
                       {"name":"CreatedById","type":"string","doc":"CreatedBy:EntityId"},
                       {"name":"Message__c","type":["null","string"],"default":null}],
             "uuid":"K98aAtiNFBjA-PJlcvaQ2g"}
            """;

        Assert.Equal("CM_Test_Event_Two__e", AvroRecordName.Parse(schema));
    }

    [Fact]
    public void Parses_a_name_only_record()
        => Assert.Equal("AccountChangeEvent", AvroRecordName.Parse("""{"type":"record","name":"AccountChangeEvent","fields":[]}"""));

    [Fact]
    public void Throws_when_the_name_is_missing()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => AvroRecordName.Parse("""{"type":"record","fields":[]}"""));
        Assert.Contains("record name", ex.Message);
    }

    [Fact]
    public void Throws_when_the_root_is_not_an_object()
        => Assert.Throws<InvalidOperationException>(() => AvroRecordName.Parse("[]"));
}
