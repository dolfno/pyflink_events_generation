import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from event_detection import EventDetection
from flink_io.measurements_source import create_measurements_env
from flink_io.sink_jdbc import create_event_sink_table, create_values_sink_table
from flink_io.tag_config_source import create_config_env
from flink_types.joined_inputs import JoinedMeasurements
from flink_types.processed_measurements import ProcessedMeasurements

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

jars = ["flink-sql-connector-kinesis_2.12-1.13.2.jar", "flink-connector-jdbc_2.12-1.13.2.jar", "postgresql-42.2.12.jar"]
jar_string_config = "".join([f"""file:///{CURRENT_DIR}/lib/{jar};""" for jar in jars])[:-1]


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_string("pipeline.jars", jar_string_config)

    # Init table sources
    t_env = create_measurements_env(t_env)
    t_env = create_config_env(t_env)

    # Create tables
    config = t_env.from_path("config")
    measurements = t_env.from_path("measurements")

    # Join source tables
    enriched_measurements = (
        measurements.where(measurements.q)
        .drop_columns(measurements.q)
        .join(config)
        .where(measurements.id == config.tag_uuid)
    )

    # From table API to stream and process Events
    ds = (
        t_env.to_append_stream(enriched_measurements, type_info=JoinedMeasurements().flink_types())
        .key_by(lambda x: x.group_id)
        .flat_map(EventDetection(), ProcessedMeasurements().flink_types())
    )

    # Temp create view to use it in table api sql DML queries
    t_env.create_temporary_view("processed_measurements", t_env.from_data_stream(ds))

    # Init sinks
    events_sink = create_event_sink_table(t_env)
    values_sink = create_values_sink_table(t_env)

    # Build insert functions and execute
    insert_events_sql = f"""INSERT INTO {events_sink} SELECT state_config_id, result_value, ts_start, ts_end FROM processed_measurements WHERE category = 'event_type' """
    insert_values_sql = f"""INSERT INTO {values_sink} SELECT value_config_id, events_id, result_value, ts_start, value_count FROM processed_measurements WHERE category = 'value_type' """
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(insert_events_sql)
    stmt_set.add_insert_sql(insert_values_sql)
    table_result = stmt_set.execute()
    table_result.wait()


if __name__ == "__main__":
    main()
