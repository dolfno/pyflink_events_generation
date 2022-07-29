from pyflink.common.typeinfo import Types


class TagConfig:
    def flink_types(self):
        return Types.ROW_NAMED(
            ["group_id", "tag_uuid", "category", "method", "config", "slot", "value_config_id"],
            [Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT()],
        )
