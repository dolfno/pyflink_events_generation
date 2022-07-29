from pyflink.table import StreamTableEnvironment


def create_measurements_env(t_env: StreamTableEnvironment):
    t_env.execute_sql(
        """
            CREATE TABLE measurements (
            `id` STRING,
            `v` STRING,
            `q` BOOLEAN,
            `t` STRING
            )
            PARTITIONED BY (id)
            WITH (
            'connector' = 'kinesis',
            'stream' = 'input-stream',
            'aws.region' = 'eu-central-1',
            'scan.stream.initpos' = 'LATEST',
            'sink.partitioner-field-delimiter' = ';',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
            )
        """
    )
    return t_env


def create_measurements_printer(t_env):
    t_env.execute_sql(
        """
            CREATE TABLE measurements_out (
            `id` STRING,
            `v` STRING,
            `q` BOOLEAN,
            `t` STRING
            )
            WITH (
              'connector' = 'print'
            )
        """
    )
    return "measurements_out"
