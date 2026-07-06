-- Replay checkpoint table for the SqlReplay store (ported from the original
-- Salesforce.Subscriber.Services.Replay.Sql). NOT auto-created — run once against the target DB
-- (CM_SalesforceReplayIdTest). The runtime principal then needs SELECT/INSERT/UPDATE.
--
-- Identity is (Application, Instance, Topic). VARCHAR keeps the clustered PK under the 900-byte
-- index limit. Times are UTC. Match [dbo].[SalesforceSubscriberReplay] to SqlReplaySettings.Schema/TableName.

IF NOT EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[SalesforceSubscriberReplay]') AND type = N'U'
)
BEGIN
    CREATE TABLE [dbo].[SalesforceSubscriberReplay] (
        [Application]       VARCHAR(128)  NOT NULL,
        [Instance]          VARCHAR(128)  NOT NULL,
        [Topic]             VARCHAR(255)  NOT NULL,
        [ReplayId]          BIGINT        NOT NULL,   -- authoritative latest persisted position
        [CreatedOn]         DATETIME2(3)  NOT NULL,
        [UpdatedOn]         DATETIME2(3)  NOT NULL,
        [LastEventReplayId] BIGINT        NULL,       -- last replay id that carried events (vs keepalive)
        [LastEventOn]       DATETIME2(3)  NULL,
        CONSTRAINT [PK_SalesforceSubscriberReplay]
            PRIMARY KEY CLUSTERED ([Application], [Instance], [Topic])
    );
END;
