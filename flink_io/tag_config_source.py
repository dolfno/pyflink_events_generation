from pyflink.table import StreamTableEnvironment


table_schema_query = """CREATE TABLE config (
            `group_id` INT,
            `tag_uuid` STRING,
            `category` STRING,
            `method` STRING,
            `config` STRING,
            `slot` INT,
            `value_config_id` INT
            )"""


def create_config_env(t_env: StreamTableEnvironment):
    t_env.execute_sql(
        f"""
            {table_schema_query}
            WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://localhost:5432/fuseplatform',
                'username' = 'FuseDatabaseAdmin',
                'password' = 'test',
                'table-name' = 'eventdetection.tags'
            )
        """
    )
    return t_env


def create_config_printer(t_env):
    t_env.execute_sql(
        f"""
            {table_schema_query}
            WITH (
              'connector' = 'print'
            )
        """
    )
    return "config_out"
