CREATE TABLE room_events_kafka
(
    id String,
    name String,
    status String,
    type String,
    channel_integration_id String,
    account_uniq_id String,
    organization_id String,
    division_id String,
    is_blocked Boolean,
    updated_at Int64
)
    ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'room-events',
         kafka_format = 'JSONEachRow',
         kafka_group_name = 'clickhouse-room-events';


CREATE TABLE room_events
(
    id String,
    name String,
    status String,
    type String,
    channel_integration_id String,
    account_uniq_id String,
    organization_id String,
    division_id String DEFAULT '',
    is_blocked Boolean DEFAULT FALSE,
    updated_at Int64
)
    ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (organization_id, id);


CREATE MATERIALIZED VIEW room_events_mv TO room_events AS
SELECT * FROM room_events_kafka;


OPTIMIZE TABLE room_events FINAL;


----------------- CUSTOM VIEWS -----------------


CREATE TABLE custom_views_kafka
(
    id String,
    name String,
    organization_id String,
    filters String, -- Store as JSON array
    updated_at Int64
)
    ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'custom-views',
         kafka_format = 'JSONEachRow',
         kafka_group_name = 'clickhouse-custom-views';


CREATE TABLE custom_views
(
    id String,
    name String,
    organization_id String,
    filters String, -- Store as JSON array
    updated_at Int64
)
    ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (organization_id, id);


CREATE MATERIALIZED VIEW custom_views_mv TO custom_views AS
SELECT * FROM custom_views_kafka;


OPTIMIZE TABLE custom_views FINAL;

----------------- CUSTOM VIEWS -----------------





----------------- MV -----------------

CREATE TABLE custom_view_count
(
    view_id String,
    organization_id String,
    event_count UInt32,
    created_at DateTime DEFAULT now()  -- Ensure partitioning column exists
)
    ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(created_at)  -- Correct deterministic partitioning
ORDER BY (organization_id, view_id);


CREATE TABLE custom_view_count_delta
(
    view_id String,
    organization_id String,
    event_count UInt32,
    updated_at DateTime DEFAULT now()
)
    ENGINE = MergeTree()
PARTITION BY toYYYYMM(updated_at)  -- Partition by month for fast cleanup
ORDER BY (updated_at, organization_id, view_id);


CREATE MATERIALIZED VIEW custom_view_count_delta_mv TO custom_view_count_delta AS
SELECT
    view_id,
    organization_id,
    sum(event_count) AS event_count,
    now() AS updated_at
FROM custom_view_count
WHERE updated_at >= now() - INTERVAL 10 SECOND  -- Only process recent changes
GROUP BY view_id, organization_id;


CREATE MATERIALIZED VIEW custom_view_count_mv TO custom_view_count AS
INSERT INTO custom_view_count
SELECT
    cv.id AS view_id,
    cv.organization_id AS organization_id,
    count() AS event_count
FROM (
         -- Step 1: Select only the latest version per (organization_id, id)
         SELECT
             id,
             organization_id,
             argMax(name, updated_at) AS name,
             argMax(status, updated_at) AS status,
             argMax(type, updated_at) AS type,
             argMax(channel_integration_id, updated_at) AS channel_integration_id,
             argMax(account_uniq_id, updated_at) AS account_uniq_id,
             argMax(division_id, updated_at) AS division_id,
             argMax(is_blocked, updated_at) AS is_blocked
         FROM room_events
         GROUP BY organization_id, id
     ) AS re
         JOIN (
    SELECT
        id,
        organization_id,
        argMax(filters, updated_at) AS filters
    FROM custom_views
    GROUP BY organization_id, id
) AS cv
              USING (organization_id)  -- Optimized join
WHERE
    arrayAll(
            f -> (
                -- Equality check (field = value)
                (JSONExtractString(f, 'operator') = 'is'
                    AND (
                     (JSONExtractString(f, 'field') = 'status' AND re.status = JSONExtractString(f, 'value')) OR
                     (JSONExtractString(f, 'field') = 'channel_integration_id' AND re.channel_integration_id = JSONExtractString(f, 'value')) OR
                     (JSONExtractString(f, 'field') = 'division_id' AND re.division_id = JSONExtractString(f, 'value'))
                     )
                    )

                    -- Not equal (field != value)
                    OR (JSONExtractString(f, 'operator') = 'is_not'
                    AND (
                            (JSONExtractString(f, 'field') = 'status' AND re.status != JSONExtractString(f, 'value')) OR
                            (JSONExtractString(f, 'field') = 'channel_integration_id' AND re.channel_integration_id != JSONExtractString(f, 'value')) OR
                            (JSONExtractString(f, 'field') = 'division_id' AND re.division_id != JSONExtractString(f, 'value'))
                            )
                    )

                    -- "IN" check (array contains value)
                    OR (JSONExtractString(f, 'operator') = 'in'
                    AND (
                            (JSONExtractString(f, 'field') = 'status' AND has(JSONExtractArrayRaw(f, 'value'), re.status)) OR
                            (JSONExtractString(f, 'field') = 'channel_integration_id' AND has(JSONExtractArrayRaw(f, 'value'), re.channel_integration_id)) OR
                            (JSONExtractString(f, 'field') = 'division_id' AND has(JSONExtractArrayRaw(f, 'value'), re.division_id))
                            )
                    )
                ),
            JSONExtractArrayRaw(cv.filters)
    )
GROUP BY cv.organization_id, cv.id;


CREATE TABLE custom_view_count_kafka
(
    view_id String,
    organization_id String,
    event_count UInt32
)
    ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'custom-view-aggregates',
         kafka_format = 'JSONEachRow',
         kafka_group_name = 'clickhouse-custom-view';


CREATE MATERIALIZED VIEW custom_view_count_kafka_mv TO custom_view_count_kafka AS
SELECT
    view_id,
    organization_id,
    event_count
FROM custom_view_count_delta
WHERE updated_at >= now() - INTERVAL 10 SECOND;  -- Only push new updates


OPTIMIZE TABLE custom_view_count FINAL;

INSERT INTO custom_view_count_kafka VALUES ('test-view', 'test-org', 10);

INSERT INTO custom_view_count_mv SELECT * FROM custom_view_count_mv;



----------------- MV -----------------




SELECT * FROM system.merges WHERE table = 'room_events_kafka';
SELECT * FROM system.parts WHERE table = 'room_events_kafka';
SELECT * FROM system.metrics WHERE metric LIKE 'Kafka%';


INSERT INTO custom_views (id, organization_id, filters)
VALUES ('view-1', 'bd30d58a-f5a1-4285-bc0e-b13d272d3a55', '[{"field": "status", "value": "resolved", "operator": "is"},{"field": "division_id", "value": null, "operator": "has_any_value"}]');


DROP MATERIALIZED VIEW custom_view_count_mv;
DROP VIEW IF EXISTS custom_view_count_mv;


-------------------------------------------------------------------------------------------------------


SELECT
    cv.id AS view_id,
    cv.organization_id AS organization_id,
    count() AS event_count
FROM (
         -- Step 1: Select only the latest version per (organization_id, id)
         SELECT
             id,
             organization_id,
             argMax(name, updated_at) AS name,
             argMax(status, updated_at) AS status,
             argMax(type, updated_at) AS type,
             argMax(channel_integration_id, updated_at) AS channel_integration_id,
             argMax(account_uniq_id, updated_at) AS account_uniq_id,
             argMax(division_id, updated_at) AS division_id,
             argMax(is_blocked, updated_at) AS is_blocked
         FROM room_events
         GROUP BY organization_id, id
     ) AS re
         JOIN (
    SELECT
        id,
        organization_id,
        argMax(filters, updated_at) AS filters
    FROM custom_views
    GROUP BY organization_id, id
) AS cv
              USING (organization_id)  -- Optimized join
WHERE
    arrayAll(
            f -> (
                -- Equality check (field = value)
                (JSONExtractString(f, 'operator') = 'is'
                    AND (
                     (JSONExtractString(f, 'field') = 'status' AND re.status = JSONExtractString(f, 'value')) OR
                     (JSONExtractString(f, 'field') = 'channel_integration_id' AND re.channel_integration_id = JSONExtractString(f, 'value')) OR
                     (JSONExtractString(f, 'field') = 'division_id' AND re.division_id = JSONExtractString(f, 'value'))
                     )
                    )

                    -- Not equal (field != value)
                    OR (JSONExtractString(f, 'operator') = 'is_not'
                    AND (
                            (JSONExtractString(f, 'field') = 'status' AND re.status != JSONExtractString(f, 'value')) OR
                            (JSONExtractString(f, 'field') = 'channel_integration_id' AND re.channel_integration_id != JSONExtractString(f, 'value')) OR
                            (JSONExtractString(f, 'field') = 'division_id' AND re.division_id != JSONExtractString(f, 'value'))
                            )
                    )

                    -- "IN" check (array contains value)
                    OR (JSONExtractString(f, 'operator') = 'in'
                    AND (
                            (JSONExtractString(f, 'field') = 'status' AND has(JSONExtractArrayRaw(f, 'value'), re.status)) OR
                            (JSONExtractString(f, 'field') = 'channel_integration_id' AND has(JSONExtractArrayRaw(f, 'value'), re.channel_integration_id)) OR
                            (JSONExtractString(f, 'field') = 'division_id' AND has(JSONExtractArrayRaw(f, 'value'), re.division_id))
                            )
                    )
                ),
            JSONExtractArrayRaw(cv.filters)
    )
GROUP BY cv.organization_id, cv.id;



-------------------------------------------------------------------------------------------------------



SELECT id, organization_id, COUNT(*)
FROM custom_views re
GROUP BY id, organization_id
HAVING COUNT(*) > 1;

SELECT * FROM room_events WHERE id='c33166df-fe1f-4831-8b31-f0e1ec0544d2';

INSERT INTO custom_view_count_mv
SELECT
    cv.id AS view_id,
    cv.organization_id AS organization_id,
    count() AS event_count
FROM room_events AS re
         JOIN custom_views AS cv
              ON cv.organization_id = re.organization_id
WHERE
    arrayAll(
            f -> (
                (JSONExtractString(f, 'field') = 'status'
                    AND JSONExtractString(f, 'operator') = 'is'
                    AND re.status = JSONExtractString(f, 'value'))
                    OR
                (JSONExtractString(f, 'field') = 'division_id'
                    AND JSONExtractString(f, 'operator') = 'has_any_value'
                    AND re.division_id IS NOT NULL)
                ),
            JSONExtractArrayRaw(cv.filters)
    )
GROUP BY cv.organization_id, cv.id;




