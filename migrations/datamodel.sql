CREATE SCHEMA IF NOT EXISTS "eventdetection";

CREATE TABLE "eventdetection"."state_config" (
    "id" serial,
    "tag_uuid" varchar NOT NULL,
    "method" varchar NOT NULL,
    "config" varchar,
    PRIMARY KEY ("id")
);

COMMENT ON COLUMN "eventdetection"."state_config"."tag_uuid" IS 'The uuid of the main tag';

COMMENT ON COLUMN "eventdetection"."state_config"."method" IS 'discrete or thresholds';

-- Add keyed lookup later.
COMMENT ON COLUMN "eventdetection"."state_config"."config" IS 'Optional configuration for the method';

CREATE TABLE "eventdetection"."events" (
    "id" serial,
    "state_config_id" int NOT NULL,
    "event_name" varchar NOT NULL,
    "ts_start" varchar NOT NULL,
    "ts_end" varchar,
    PRIMARY KEY ("id"),
    UNIQUE ("state_config_id", "ts_start"),
    CONSTRAINT fk_state_config FOREIGN KEY ("state_config_id") REFERENCES eventdetection.state_config ("id") ON DELETE CASCADE
);

CREATE TABLE "eventdetection"."value_config" (
    "id" serial,
    "state_config_id" int NOT NULL,
    "tag_uuid" varchar NOT NULL,
    "aggregation" varchar NOT NULL,
    "config" varchar,
    "slot" int NOT NULL,
    CHECK (slot < 10),
    PRIMARY KEY ("id"),
    CONSTRAINT fk_state_config FOREIGN KEY ("state_config_id") REFERENCES eventdetection.state_config ("id") ON DELETE CASCADE
);

COMMENT ON COLUMN "eventdetection"."value_config"."tag_uuid" IS 'The uuid of the main tag';

COMMENT ON COLUMN "eventdetection"."value_config"."aggregation" IS 'mean, max, min';

-- Add keyed lookup later or lambda function.
COMMENT ON COLUMN "eventdetection"."value_config"."config" IS 'Optional configuration for the aggregation method';

-- Table Definition
CREATE TABLE "eventdetection"."values" (
    "id" serial,
    "value_config_id" int NOT NULL,
    "events_id" varchar NOT NULL, -- currently just concat(group_id, event.ts_start)
    "value_result" varchar NOT NULL,
    "last_updated" bigint NOT NULL,
    CONSTRAINT "fk_value_config" FOREIGN KEY ("value_config_id") REFERENCES "eventdetection"."value_config" ("id") ON DELETE CASCADE,
    -- CONSTRAINT "fk_events_id" FOREIGN KEY ("events_id") REFERENCES "eventdetection"."events" ("id") ON DELETE CASCADE,
    PRIMARY KEY ("id")
);

CREATE VIEW "eventdetection"."tags" AS (
    SELECT
        "id" AS "group_id",
        "tag_uuid",
        'event_type' AS "category",
        "method",
        "config",
        NULL AS "slot"
    FROM
        eventdetection.state_config
    UNION
    SELECT
        "state_config_id" AS "group_id",
        "tag_uuid",
        'value_type' AS "category",
        "aggregation" AS "method",
        "config",
        "slot"
    FROM
        eventdetection.value_config)
