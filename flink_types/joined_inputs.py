from pyflink.common.typeinfo import Types


class JoinedMeasurements:
    def flink_types(self):
        return Types.ROW_NAMED(
            ["id", "v", "t", "group_id", "tag_uuid", "category", "method", "config", "slot", "value_config_id"],
            [
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.INT(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.INT(),
                Types.INT(),
            ],
        )
