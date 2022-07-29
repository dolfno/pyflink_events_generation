from pyflink.datastream.connectors import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.typeinfo import Types


def create_event_sink_table(t_env):
    sink_name = "events_sink"

    t_env.execute_sql(
        f"""
            CREATE TABLE {sink_name} (
            `state_config_id` INT,
            `event_name` STRING,
            `ts_start` BIGINT,
            `ts_end` BIGINT,
            CONSTRAINT events_pkey PRIMARY KEY (state_config_id, ts_start) NOT ENFORCED
            )
            WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://localhost:5432/fuseplatform',
                'username' = 'FuseDatabaseAdmin',
                'password' = 'test',
                'table-name' = 'eventdetection.events'
            )
        """
    )
    return sink_name


# CONSTRAINT events_pkey PRIMARY KEY (value_config_id, state_config_id) NOT ENFORCED
def create_values_sink_table(t_env):
    sink_name = "values_sink"

    t_env.execute_sql(
        f"""
            CREATE TABLE {sink_name} (
            `value_config_id` INT,
            `events_id` STRING,
            `value_result` STRING,
            `last_updated` BIGINT,
            `value_count` INT
            )
            WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://localhost:5432/fuseplatform',
                'username' = 'FuseDatabaseAdmin',
                'password' = 'test',
                'table-name' = 'eventdetection.values'
            )
        """
    )
    return sink_name


def create_event_sink_printer(t_env):
    t_env.execute_sql(
        """
            CREATE TABLE events (
            `state_config_id` INT,
            `event_name` STRING,
            `ts_start` BIGINT,
            `ts_end` BIGINT
            )
            WITH (
              'connector' = 'print'
            )
        """
    )
    return "events"


event_types = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.STRING()])


def create_postgres_sink():
    insert_query = (
        """INSERT INTO into eventdetection.events (state_id, event_name, ts_start, ts_end) values (?, ?, ?, ?) 
                        ON CONFLICT (events_pkey) 
                        DO UPDATE SET event_name = EXCLUDED.event_name, ts_end = EXCLUDED.ts_end;""",
    )
    jdbc_con_opts = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url("jdbc:postgresql://localhost:5432/fuseplatform")
        .with_user_name("FuseDatabaseAdmin")
        .with_password("test")
        .with_driver_name("/Users/dnoordman/pyflink_events_generation/lib/postgresql-42.2.12.jar")
        .build()
    )
    build_opts = (
        JdbcExecutionOptions.Builder().with_batch_interval_ms(1000).with_batch_size(2).with_max_retries(3).build()
    )
    return JdbcSink.sink(
        insert_query,
        type_info=event_types,
        jdbc_connection_options=jdbc_con_opts,
        jdbc_execution_options=build_opts,
    )
